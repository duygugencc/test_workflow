import datetime
import decimal
import hashlib
import itertools
import json
import logging
from pathlib import Path

import google.auth
import yaml
from google.auth import impersonated_credentials
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


def chunked(iterable, size):
    """Yields iterable in lists of at most size items each."""
    it = iter(iterable)
    while True:
        chunk = list(itertools.islice(it, size))
        if not chunk:
            return
        yield chunk


def load_yaml_file(file_path):
    if not Path(file_path).exists():
        raise ValueError(f"File '{file_path}' does not exist.")
    with open(file_path, "r") as f:
        return yaml.safe_load(f)


def resolve_read_credentials(impersonate_sa: str = None):
    """Credentials for reading from Bloomreach's gcloudltds project.

    Impersonates the given service account (normally
    sa-bloomreach-cdp-prod, the only identity with access to the Bloomreach
    export) if one is passed in, otherwise just uses the flow's own
    default credentials."""
    base_credentials, _ = google.auth.default()
    if not impersonate_sa:
        return base_credentials
    return impersonated_credentials.Credentials(
        source_credentials=base_credentials,
        target_principal=impersonate_sa,
        target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
        lifetime=300,
    )


def _json_safe(value):
    """Converts BigQuery row values (datetime, Decimal, nested structs,
    repeated fields) into plain types that json.dumps() can handle, so
    bq_ingestor can serialize each record directly."""
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def rows_to_json_safe_dicts(rows) -> list:
    return [_json_safe(dict(row)) for row in rows]


def normalize_name(name: str) -> str:
    """Lowercase and strip underscores, for loose name matching (e.g.
    comparing a column name against a policy tag taxonomy display_name)."""
    return name.lower().replace("_", "")


def entity_name(table_name: str, market_code: str, env: str) -> str:
    """Destination entity name: {table}_{market}_br, with a _DEV suffix in
    the dev environment only."""
    name = f"{table_name}_{market_code}_br"
    if env == "dev":
        name += "_DEV"
    return name


def flatten_nested_fields(record: dict, fields_to_flatten=None) -> dict:
    """Turns a nested struct field (e.g. "raw_properties") into separate
    top-level fields, one per sub-key, prefixed with the parent name (e.g.
    "raw_properties_brand", "raw_properties_campaign_id").
    """
    flattened = dict(record)

    for field_name in (fields_to_flatten or []):
        value = flattened.get(field_name)
        if not isinstance(value, dict):
            continue
        del flattened[field_name]
        for sub_key, sub_value in value.items():
            flattened[f"{field_name}_{sub_key}"] = sub_value
    return flattened


def compute_row_hash(record: dict) -> str:
    """Hashes a record the same way bq_ingestor does internally, so we can
    compare a freshly-read record against row_hash values already stored
    in a final table. Must be kept in sync with bq_ingestor's own hashing
    logic if that ever changes."""
    return hashlib.sha256(json.dumps(record, sort_keys=True).encode("utf-8")).hexdigest()


def ensure_final_table_schema(client, final_table, all_keys, cluster_fields):
    """Creates final_table (all-STRING columns for all_keys, plus
    row_hash/ingested_at/is_deleted/is_anonymised) if it doesn't exist yet,
    or adds any newly-seen columns as STRING if it does. Standalone
    equivalent of bq_ingestor's own update_final_table_schema, needed
    because this table bypasses bq_ingestor entirely (see
    stage_full_records/merge_staging_into_final)."""
    try:
        table = client.get_table(final_table)
    except NotFound:
        schema = [bigquery.SchemaField(k, "STRING") for k in sorted(all_keys)]
        schema += [
            bigquery.SchemaField("row_hash", "STRING"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
            bigquery.SchemaField("is_deleted", "BOOLEAN"),
            bigquery.SchemaField("is_anonymised", "STRING"),
        ]
        table = bigquery.Table(final_table, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="ingested_at"
        )
        table.clustering_fields = cluster_fields
        client.create_table(table)
        return

    existing_fields = {f.name for f in table.schema}
    new_fields = set(all_keys) - existing_fields
    if new_fields:
        updated_schema = table.schema[:] + [bigquery.SchemaField(k, "STRING") for k in sorted(new_fields)]
        table.schema = updated_schema
        client.update_table(table, ["schema"])


def resolve_flattened_schema(client, full_table, drop_fields=None, flatten_fields=None, logger=None):
    """The post-flatten column list for full_table, resolved from its
    schema metadata alone — no row data read. Safe to call upfront since
    every row from the same table/view has the same columns; mirrors what
    flatten_nested_fields would produce per-record, but done once here so
    read_and_stage_source_rows can chunk the actual row data without
    needing to see it all first to know what columns it'll produce."""
    logger = logger or logging.getLogger(__name__)
    table = client.get_table(full_table)
    drop_fields = set(drop_fields or [])
    flatten_fields = set(flatten_fields or [])
    matched = set()
    keys = []
    for field in table.schema:
        if field.name in drop_fields:
            continue
        if field.name in flatten_fields and field.field_type in ("RECORD", "STRUCT"):
            keys.extend(f"{field.name}_{sub.name}" for sub in field.fields)
            matched.add(field.name)
        else:
            keys.append(field.name)

    unmatched = flatten_fields - matched
    if unmatched:
        logger.warning(
            f"{full_table}'s schema doesn't contain a RECORD/STRUCT field for {sorted(unmatched)} — "
            f"configured in flatten_fields but missing, or not a nested field, so nothing was flattened"
        )
    return sorted(keys)


def stage_full_records(client, staging_table, records, all_keys, append=False):
    """Loads a batch of the fresh pull (every field cast to STRING, plus
    row_hash/ingested_at) into a staging table — truncating on the first
    call (append=False) and appending on subsequent calls, so
    read_and_stage_source_rows can stream the source table in one chunk
    at a time rather than needing the full pull in memory at once.
    merge_staging_into_final upserts final_table from the complete result."""
    ingested_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    rows = []
    for record in records:
        row = {k: (None if record.get(k) is None else str(record[k])) for k in all_keys}
        row["row_hash"] = compute_row_hash(record)
        row["ingested_at"] = ingested_at
        rows.append(row)

    schema = [bigquery.SchemaField(k, "STRING") for k in all_keys]
    schema += [
        bigquery.SchemaField("row_hash", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]
    write_disposition = "WRITE_APPEND" if append else "WRITE_TRUNCATE"
    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition=write_disposition)
    client.load_table_from_json(rows, staging_table, job_config=job_config).result()


def build_merge_query(final_table, staging_table, all_keys):
    """Upserts staging_table (a full fresh snapshot, one row per row_hash)
    into final_table in a single statement: inserts row_hash values not
    yet in final_table, and flags any final_table row_hash missing from
    staging_table as deleted. Replaces bq_ingestor's separate raw-write +
    merge, plus our own deleted-row flagging, with one MERGE."""
    insert_cols = ", ".join(all_keys)
    insert_vals = ", ".join(f"S.{k}" for k in all_keys)
    return f"""
    MERGE INTO `{final_table}` T
    USING `{staging_table}` S
    ON T.row_hash = S.row_hash
    WHEN NOT MATCHED THEN
    INSERT ({insert_cols}, row_hash, ingested_at, is_deleted)
    VALUES ({insert_vals}, S.row_hash, S.ingested_at, FALSE)
    WHEN NOT MATCHED BY SOURCE AND IFNULL(T.is_deleted, FALSE) != TRUE THEN
    UPDATE SET is_deleted = TRUE, ingested_at = CURRENT_TIMESTAMP()
    """


def merge_staging_into_final(client, final_table, staging_table, all_keys):
    client.query(build_merge_query(final_table, staging_table, all_keys)).result()


def _policy_tag_resource(taxonomy_project, taxonomy_location, taxonomy_id, policy_tag_id):
    return (
        f"projects/{taxonomy_project}/locations/{taxonomy_location}"
        f"/taxonomies/{taxonomy_id}/policyTags/{policy_tag_id}"
    )


def fetch_policy_tag_mapping(client, taxonomy_project, taxonomy_dataset, metadata_table, taxonomy_table,
                              taxonomy_ids, taxonomy_location="europe-north1"):
    """Builds a {normalized_display_name: policy_tag_resource_path} lookup
    from the Data Catalog taxonomy metadata tables."""
    query = f"""
    SELECT t1.taxonomy_id, t1.display_name, t1.policy_tag_id, t2.taxonomy_display_name AS taxonomy_name
    FROM `{taxonomy_project}.{taxonomy_dataset}.{metadata_table}` t1
    JOIN `{taxonomy_project}.{taxonomy_dataset}.{taxonomy_table}` t2
    ON t1.taxonomy_id = t2.id
    WHERE t1.taxonomy_id IN UNNEST(@taxonomy_ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("taxonomy_ids", "STRING", list(taxonomy_ids))]
    )
    rows = client.query(query, job_config=job_config).result()

    mapping = {}
    for row in rows:
        tag_name = normalize_name(row["display_name"])
        mapping[tag_name] = _policy_tag_resource(
            taxonomy_project, taxonomy_location, row["taxonomy_id"], row["policy_tag_id"]
        )
    return mapping


def _is_full_resource_path(value):
    return value.startswith("projects/")


def _resolve_pii_tag(value, policy_tag_mapping):
    """A pii_fields value is either a full resource path (used directly)
    or a plain display name (looked up in policy_tag_mapping)."""
    if _is_full_resource_path(value):
        return value
    return policy_tag_mapping.get(normalize_name(value))


def table_needs_policy_tag_mapping(table_config):
    """Whether this table will actually need the shared taxonomy mapping
    (fetch_policy_tag_mapping) — true if it has no pii_fields configured
    (falls back to scanning every column), or if any pii_fields value is a
    plain display name rather than a full resource path. Lets the flow
    skip fetching the mapping entirely when nothing would use it."""
    if not table_config.get("apply_policy_tags"):
        return False
    pii_fields = table_config.get("pii_fields")
    if not pii_fields:
        return True
    return any(not _is_full_resource_path(v) for v in pii_fields.values())


def _match_candidates(field_name, flatten_prefixes):
    """A column's own name, plus that name with any known flatten prefix
    stripped off (e.g. "raw_properties_city" also tries "city"). Used by
    the automatic tagging fallback."""
    candidates = [field_name]
    for prefix in flatten_prefixes:
        if field_name.startswith(prefix):
            candidates.append(field_name[len(prefix):])
    return candidates


def _auto_tag_for_field(field_name, policy_tag_mapping, flatten_prefixes):
    for candidate in _match_candidates(field_name, flatten_prefixes):
        tag = _resolve_pii_tag(candidate, policy_tag_mapping)
        if tag:
            return tag
    return None


def _already_tagged(field, tag):
    current = set(field.policy_tags.names) if field.policy_tags else set()
    return tag in current


def _walk_and_tag_schema(schema, resolve_tag):
    """Walks the schema (recursing into nested structs), calling
    resolve_tag(field_name) on each leaf field to get its target tag (or
    None). Returns (new_schema, changed) — changed is False if every field
    already had the tag resolve_tag picked, so this is safe to call
    repeatedly without causing needless schema updates."""
    updated = []
    changed = False
    for field in schema:
        if field.field_type in ("RECORD", "STRUCT") and field.fields:
            nested, nested_changed = _walk_and_tag_schema(field.fields, resolve_tag)
            changed = changed or nested_changed
            updated.append(bigquery.SchemaField(
                field.name, field.field_type, mode=field.mode, fields=nested, description=field.description,
            ))
            continue

        tag = resolve_tag(field.name)
        if tag and not _already_tagged(field, tag):
            updated.append(bigquery.SchemaField(
                field.name, field.field_type, mode=field.mode, description=field.description,
                policy_tags=bigquery.PolicyTagList(names=[tag]),
            ))
            changed = True
        else:
            updated.append(field)
    return updated, changed


def build_tagged_schema_for_pii_fields(schema, pii_fields, policy_tag_mapping):
    """Tags only the columns listed in pii_fields (a
    {column_name: resource_path_or_display_name} mapping from config) —
    nothing else in the schema is touched."""
    def resolve_tag(field_name):
        value = pii_fields.get(field_name)
        return _resolve_pii_tag(value, policy_tag_mapping) if value else None

    return _walk_and_tag_schema(schema, resolve_tag)


def build_tagged_schema(schema, policy_tag_mapping, flatten_prefixes):
    """Fallback for tables with no pii_fields configured: scans every
    column and tags whatever matches the taxonomy by name (after
    stripping known prefixes and normalizing). Best-effort — can produce
    false positives, so a curated pii_fields entry is preferred once a
    table's real PII columns are known."""
    return _walk_and_tag_schema(
        schema,
        lambda field_name: _auto_tag_for_field(field_name, policy_tag_mapping, flatten_prefixes),
    )


def apply_policy_tags(client, table_id, policy_tag_mapping, flatten_prefixes, pii_fields=None, logger=None):
    """Fetch table_id's current schema and apply policy tags — idempotent,
    safe to call on every run: only pushes a schema update if something is
    actually missing a tag."""
    logger = logger or logging.getLogger(__name__)
    table = client.get_table(table_id)
    if pii_fields:
        new_schema, changed = build_tagged_schema_for_pii_fields(table.schema, pii_fields, policy_tag_mapping)
    else:
        new_schema, changed = build_tagged_schema(table.schema, policy_tag_mapping, flatten_prefixes)

    if not changed:
        logger.info(f"Policy tags already up to date on {table_id}, nothing to do")
        return

    table.schema = new_schema
    client.update_table(table, ["schema"])
    logger.info(f"Applied policy tags to {table_id}")
