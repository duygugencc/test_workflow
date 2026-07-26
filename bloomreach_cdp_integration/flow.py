import datetime
import gc
import logging
from types import SimpleNamespace

from bq_ingestor import BigQueryDataIngestor
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run
from prefect.tasks import exponential_backoff

from helpers import (
    apply_policy_tags,
    chunked,
    ensure_final_table_schema,
    entity_name,
    fetch_policy_tag_mapping,
    flatten_nested_fields,
    load_yaml_file,
    merge_staging_into_final,
    resolve_flattened_schema,
    resolve_read_credentials,
    rows_to_json_safe_dicts,
    stage_full_records,
    table_needs_policy_tag_mapping,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

# Policy tags are disabled while still being validated. Flip to True to
# re-enable — the code is left in place, not removed.
POLICY_TAGS_ENABLED = False

# Fallback row count per chunk for read_and_stage_source_rows, if a table
# doesn't set its own chunk_size in config.yml.
DEFAULT_CHUNK_SIZE = 50_000


@task(name="resolve_source_schema", task_run_name="resolve_source_schema-{table_name}-{market_code}")
def resolve_source_schema_task(credentials, source_project, source_dataset, table_name, market_code,
                                drop_fields, flatten_fields):
    client = bigquery.Client(project=source_project, credentials=credentials)
    full_table = f"{source_project}.{source_dataset}.{table_name}"
    return resolve_flattened_schema(client, full_table, drop_fields, flatten_fields, logger=get_run_logger())


@task(name="read_and_stage_source_rows", task_run_name="read_and_stage_source_rows-{table_name}-{market_code}",
      retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=10))
def read_and_stage_source_rows(credentials, source_project, source_dataset, table_name, market_code,
                                dest_client, staging_table, all_keys, flatten_fields, drop_fields=None,
                                chunk_size=DEFAULT_CHUNK_SIZE):
    """Reads table_name and appends it into staging_table in chunk_size
    batches — only one chunk's worth of records is ever in memory at a time."""
    logger = get_run_logger()
    entity = f"{table_name}_{market_code}"
    full_table = f"{source_project}.{source_dataset}.{table_name}"
    select_clause = f"* EXCEPT({', '.join(drop_fields)})" if drop_fields else "*"
    query = f"SELECT {select_clause} FROM `{full_table}`"
    source_client = bigquery.Client(project=source_project, credentials=credentials)
    logger.info(f"[{entity}] reading from Bloomreach CDP (full table, chunked)")
    rows = source_client.query(query).result(page_size=chunk_size)
    num_batches = -(-rows.total_rows // chunk_size) if rows.total_rows else 0
    logger.info(f"[{entity}] fetched {rows.total_rows} row(s)")

    total = 0
    for i, chunk in enumerate(chunked(rows, chunk_size), start=1):
        records = _flatten_in_place(rows_to_json_safe_dicts(chunk), flatten_fields)
        stage_full_records(dest_client, staging_table, records, all_keys, append=(i > 1))
        total += len(records)
        logger.info(f"[{entity}] staged BATCH {i}/{num_batches or '?'} ({total} row(s) so far)")
        del records
        gc.collect()

    logger.info(f"[{entity}] read and staged {total} row(s)")
    return total


def _flatten_in_place(records, flatten_fields):
    """Flattens records in place (overwriting each element as it goes)
    rather than building a second full-size list via a comprehension —
    building a whole new list would momentarily hold both the original
    and flattened versions of every record at once, doubling peak memory
    for no reason."""
    if not flatten_fields:
        return records
    for i in range(len(records)):
        records[i] = flatten_nested_fields(records[i], flatten_fields)
    return records


def _build_ingestor(name, raw_table, final_table, primary_key, cluster_fields,
                     infer_deleted_from_full_load, logger):
    return BigQueryDataIngestor(
        name=name,
        raw_table=raw_table,
        final_table=final_table,
        primary_key=primary_key,
        cluster_fields=cluster_fields,
        infer_deleted_from_full_load=infer_deleted_from_full_load,
        logger=logger,
    )


def _write_records_to_raw(name, raw_table, final_table, primary_key, cluster_fields, records,
                           infer_deleted_from_full_load, logger):
    ingestor = _build_ingestor(name, raw_table, final_table, primary_key, cluster_fields,
                                infer_deleted_from_full_load, logger)
    return ingestor.process(records) or []


@task(name="read_and_write_source_chunks", task_run_name="read_and_write_source_chunks-{table_name}-{market_code}",
      retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=10))
def read_and_write_source_chunks(credentials, source_project, source_dataset, table_name, market_code,
                                  incremental_field, window_start, window_end, drop_fields, flatten_fields,
                                  name, raw_table, final_table, primary_key, cluster_fields,
                                  infer_deleted_from_full_load, chunk_size):
    """Reads table_name in chunk_size batches and writes each batch to
    raw immediately (bq_ingestor's process(), called once per chunk), so
    the full window is never materialized as one Python list. bq_ingestor
    already supports calling process() multiple times, accumulating
    schema, before a single merge_to_final call — see process_table. This
    stays one task (not a plain function calling per-chunk tasks) so it
    can call the per-chunk write directly without tripping Prefect's
    tasks-can't-call-tasks restriction — _write_records_to_raw is a plain
    function here, not a task."""
    logger = get_run_logger()
    entity = f"{table_name}_{market_code}"
    full_table = f"{source_project}.{source_dataset}.{table_name}"
    select_clause = f"* EXCEPT({', '.join(drop_fields)})" if drop_fields else "*"
    query = f"SELECT {select_clause} FROM `{full_table}`"
    query_parameters = []

    if incremental_field:
        query += f" WHERE `{incremental_field}` >= @window_start AND `{incremental_field}` < @window_end"
        query_parameters = [
            bigquery.ScalarQueryParameter("window_start", "TIMESTAMP", window_start),
            bigquery.ScalarQueryParameter("window_end", "TIMESTAMP", window_end),
        ]
        logger.info(f"[{entity}] reading from Bloomreach CDP (window {window_start} -> {window_end}, chunked)")
    else:
        logger.info(f"[{entity}] reading from Bloomreach CDP (full table, chunked)")

    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    client = bigquery.Client(project=source_project, credentials=credentials)
    rows = client.query(query, job_config=job_config).result(page_size=chunk_size)
    num_batches = -(-rows.total_rows // chunk_size) if rows.total_rows else 0
    logger.info(f"[{entity}] fetched {rows.total_rows} row(s)")

    all_new_keys = set()
    total = 0
    for i, chunk in enumerate(chunked(rows, chunk_size), start=1):
        records = _flatten_in_place(rows_to_json_safe_dicts(chunk), flatten_fields)
        new_keys = _write_records_to_raw(
            name, raw_table, final_table, primary_key, cluster_fields, records,
            infer_deleted_from_full_load, logger,
        )
        all_new_keys.update(new_keys)
        total += len(records)
        logger.info(f"[{entity}] wrote BATCH {i}/{num_batches or '?'} ({total} row(s) so far)")
        del records
        gc.collect()

    logger.info(f"[{entity}] read and wrote {total} row(s)")
    return total, sorted(all_new_keys)


@task(name="merge_to_final", task_run_name="merge_to_final-{name}",
      retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=10))
def merge_to_final(name, raw_table, final_table, primary_key, cluster_fields, new_keys, start_timestamp,
                    infer_deleted_from_full_load=False):
    """Merges the raw table's new/changed rows into the final table."""
    logger = get_run_logger()
    ingestor = _build_ingestor(name, raw_table, final_table, primary_key, cluster_fields,
                                infer_deleted_from_full_load, logger)
    ingestor.unnest_to_bigquery(new_keys=new_keys, start_timestamp=start_timestamp)


@task(name="check_table_exists", task_run_name="check_table_exists-{table_id}")
def check_table_exists(dest_client, table_id):
    try:
        dest_client.get_table(table_id)
        return True
    except NotFound:
        return False


@task(name="add_is_anonymised_column", task_run_name="add_is_anonymised_column-{table_id}")
def add_is_anonymised_column(dest_client, table_id):
    logger = get_run_logger()
    dest_client.query(f"ALTER TABLE `{table_id}` ADD COLUMN IF NOT EXISTS is_anonymised STRING").result()
    logger.info(f"Ensured is_anonymised STRING column (default NULL) on {table_id}")


@task(name="ensure_final_table_schema", task_run_name="ensure_final_table_schema-{final_table}")
def ensure_final_table_schema_task(dest_client, final_table, all_keys, cluster_fields):
    ensure_final_table_schema(dest_client, final_table, all_keys, cluster_fields)


@task(name="merge_staging_into_final", task_run_name="merge_staging_into_final-{final_table}",
      retries=3, retry_delay_seconds=exponential_backoff(backoff_factor=10))
def merge_staging_into_final_task(dest_client, final_table, staging_table, all_keys):
    merge_staging_into_final(dest_client, final_table, staging_table, all_keys)


@task(name="fetch_policy_tag_mapping")
def fetch_policy_tag_mapping_task(dest_client, policy_tags_config):
    return fetch_policy_tag_mapping(
        dest_client,
        taxonomy_project=policy_tags_config["taxonomy_project"],
        taxonomy_dataset=policy_tags_config["taxonomy_dataset"],
        metadata_table=policy_tags_config["metadata_table"],
        taxonomy_table=policy_tags_config["taxonomy_table"],
        taxonomy_ids=policy_tags_config["taxonomy_ids"],
        taxonomy_location=policy_tags_config.get("taxonomy_location", "europe-north1"),
    )


@task(name="apply_policy_tags", task_run_name="apply_policy_tags-{table_id}")
def apply_policy_tags_task(dest_client, table_id, policy_tag_mapping, policy_tags_config, pii_fields=None):
    logger = get_run_logger()
    apply_policy_tags(
        dest_client, table_id, policy_tag_mapping,
        flatten_prefixes=policy_tags_config.get("flatten_prefixes", []),
        pii_fields=pii_fields,
        logger=logger,
    )


# ---------------------------------------------------------------------------
# process_table helpers
# ---------------------------------------------------------------------------

def _upsert_via_staging_merge(ctx, source_dataset, source_table, market_code, final_table, table_config):
    """For dedupe_raw_writes tables: upserts the full fresh pull directly
    into final_table via one staging table + one MERGE (see
    stage_full_records/merge_staging_into_final), instead of writing
    through bq_ingestor's raw table and separate merge step."""
    drop_fields = table_config.get("drop_fields")
    flatten_fields = table_config.get("flatten_fields")
    all_keys = resolve_source_schema_task(
        ctx.read_credentials, ctx.source_project, source_dataset, source_table, market_code,
        drop_fields, flatten_fields,
    )
    cluster_fields = table_config.get("cluster_fields", ["row_hash"])
    ensure_final_table_schema_task(ctx.dest_client, final_table, all_keys, cluster_fields)

    staging_table = f"{final_table}_staging_tmp"
    total = read_and_stage_source_rows(
        ctx.read_credentials, ctx.source_project, source_dataset, source_table, market_code,
        ctx.dest_client, staging_table, all_keys, flatten_fields, drop_fields=drop_fields,
        chunk_size=table_config.get("chunk_size", DEFAULT_CHUNK_SIZE),
    )
    if total > 0:
        merge_staging_into_final_task(ctx.dest_client, final_table, staging_table, all_keys)
    ctx.dest_client.delete_table(staging_table, not_found_ok=True)
    return total > 0


def _add_extra_columns(dest_client, final_table, final_table_existed, any_records):
    if final_table_existed or not any_records:
        return
    add_is_anonymised_column(dest_client, final_table)


def _tag_table(ctx, table_config, final_table, any_records):
    if not (POLICY_TAGS_ENABLED and any_records and table_config.get("apply_policy_tags")
            and ctx.policy_tags_config):
        return
    apply_policy_tags_task(
        ctx.dest_client, final_table, ctx.policy_tag_mapping, ctx.policy_tags_config,
        pii_fields=table_config.get("pii_fields"),
    )


def process_table(table_name, table_config, market_code, source_dataset, ctx):
    source_table = table_config.get("source_table", table_name)
    incremental_field = None if ctx.is_backfill else table_config.get("incremental_field")

    table_entity_name = entity_name(table_name, market_code, ctx.env)
    raw_table = f"{ctx.dest_project}.{ctx.dataset}.{table_entity_name}_raw"
    final_table = f"{ctx.dest_project}.{ctx.dataset}.{table_entity_name}"

    final_table_existed = check_table_exists(ctx.dest_client, final_table)
    dedupe_raw_writes = table_config.get("dedupe_raw_writes")

    if dedupe_raw_writes:
        any_records = _upsert_via_staging_merge(
            ctx, source_dataset, source_table, market_code, final_table, table_config,
        )
    else:
        total, new_keys = read_and_write_source_chunks(
            ctx.read_credentials, ctx.source_project, source_dataset, source_table, market_code,
            incremental_field,
            ctx.window_start if incremental_field else None,
            ctx.window_end if incremental_field else None,
            table_config.get("drop_fields"), table_config.get("flatten_fields"),
            table_entity_name, raw_table, final_table,
            table_config.get("primary_key", "row_hash"), table_config.get("cluster_fields", ["row_hash"]),
            table_config.get("infer_deleted_from_full_load", False),
            table_config.get("chunk_size", DEFAULT_CHUNK_SIZE),
        )
        any_records = total > 0

        if any_records:
            merge_to_final(
                name=table_entity_name,
                raw_table=raw_table,
                final_table=final_table,
                primary_key=table_config.get("primary_key", "row_hash"),
                cluster_fields=table_config.get("cluster_fields", ["row_hash"]),
                new_keys=new_keys,
                start_timestamp=ctx.start_timestamp,
                infer_deleted_from_full_load=table_config.get("infer_deleted_from_full_load", False),
            )
        _add_extra_columns(ctx.dest_client, final_table, final_table_existed, any_records)

    _tag_table(ctx, table_config, final_table, any_records)


def process_market(market_code, source_dataset, tables, ctx):
    """Runs all tables for one market, one after another.

    Deliberately a plain function, not a @task — process_table calls
    several tasks internally, and Prefect doesn't allow a task to call
    another task. A plain function has no such restriction."""
    for table_name, table_config in tables.items():
        process_table(table_name, table_config, market_code, source_dataset, ctx)


@flow(name="bloomreach_cdp_integration", flow_run_name="bloomreach_cdp_integration-{env}-{market_code}")
def flow(env: str = "dev", backfill_start: str = None, backfill_end: str = None, is_backfill: bool = False,
         market_code: str = "all", table_names: list = None):
    """Main entrypoint:
    backfill_start / backfill_end: override the normal rolling daily
    window, for tables that have an incremental_field configured
    (currently just campaign). Still windowed, just over a different range.

    is_backfill: a full pull with no windowing at all, for every table.
    More drastic than backfill_start/backfill_end — not the same setting.
    """
    config = load_yaml_file("config.yml")[env]
    source_project = config["source_project"]
    dest_project = config["dest_project"]
    dataset = config["dataset"]
    all_markets = config["markets"]
    all_tables = config["tables"]
    markets = {market_code: all_markets[market_code]} if market_code != "all" else all_markets
    tables = {name: all_tables[name] for name in table_names} if table_names else all_tables
    policy_tags_config = config.get("policy_tags")

    read_credentials = resolve_read_credentials(config.get("read_impersonate_sa"))
    dest_client = bigquery.Client(project=dest_project)


    now_anchor = flow_run.scheduled_start_time

    start_timestamp = now_anchor - datetime.timedelta(minutes=10)
    window_end = datetime.datetime.fromisoformat(backfill_end) if backfill_end else now_anchor
    window_start = datetime.datetime.fromisoformat(backfill_start) if backfill_start else window_end - datetime.timedelta(days=1)

    # Skip fetching the shared taxonomy mapping if no table would use it
    # (see table_needs_policy_tag_mapping).
    policy_tag_mapping = None
    if POLICY_TAGS_ENABLED and policy_tags_config and any(table_needs_policy_tag_mapping(t) for t in tables.values()):
        policy_tag_mapping = fetch_policy_tag_mapping_task(dest_client, policy_tags_config)

    # Bundled into one object so process_market/process_table don't need a
    # long, growing parameter list for every non-market-specific value.
    ctx = SimpleNamespace(
        source_project=source_project, dest_project=dest_project, dataset=dataset, env=env,
        is_backfill=is_backfill, read_credentials=read_credentials, dest_client=dest_client,
        window_start=window_start, window_end=window_end, start_timestamp=start_timestamp,
        policy_tags_config=policy_tags_config, policy_tag_mapping=policy_tag_mapping,
    )

    # Markets run one at a time — running all three at once was OOM-killing the job.
    for mc, market_config in markets.items():
        process_market(mc, market_config["source_dataset"], tables, ctx)


if __name__ == "__main__":
    flow(env="dev")
