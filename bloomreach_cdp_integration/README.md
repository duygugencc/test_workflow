# Bloomreach CDP Integration

Prefect flow that ingests Bloomreach CDP data (per market) from BigQuery into
`sambla-data-staging-compliance`, using the shared `bq_ingestor` package for
the raw-write + merge-to-final pattern.

## How it runs

- Scheduled daily at 05:00, ahead of the 06:15 dbt run.
- Reads from `gcloudltds` by impersonating
  `sa-bloomreach-cdp-prod@data-domain-data-warehouse.iam.gserviceaccount.com`
  (the only identity granted access to the Bloomreach EBQ export). This
  impersonation is used **only for reads**.
- Writes (raw table creation, merge, DDL, policy tags) all use whatever
  identity actually runs `flow.py` — your own `gcloud` ADC when run locally,
  or the Prefect worker/deployment's service account in a deployed
  environment. `bq_ingestor`'s own BigQuery client (`bigquery.Client()`,
  no explicit credentials) and `flow.py`'s `dest_client` both rely on
  ambient default credentials.
- The three markets (`se`, `no`, `fi`) are independent of each other and run
  **concurrently** — each market is submitted as its own `process_market`
  Prefect task via `.submit()`. Tables within a market still run one after
  another (real dependency: raw write → merge → delete-flagging → policy
  tags).

## Table naming

Destination entity name: `{table_name}_{market_code}_br`, with a `_DEV`
suffix appended in the `dev` environment only (never in `prod`). E.g.
`campaign_se_br_DEV` / `campaign_se_br_DEV_raw` in dev,
`campaign_se_br` / `campaign_se_br_raw` in prod.

## config.yml structure

Three shared blocks (`markets`, `policy_tags`, `tables`) are defined once at
the top level using YAML anchors (`&markets`, `&policy_tags`, `&tables`) and
reused via aliases (`*markets`, etc.) in both the `dev` and `prod`
environment blocks, so market/table/policy-tag definitions never have to be
duplicated between environments.

### `markets`

`{market_code: {source_dataset: ...}}`. **Note:** `no` (Norway) must stay
quoted (`"no":`) — unquoted, YAML 1.1 parses it as the boolean `False`
instead of the string `"no"`, which silently breaks the destination table
name (`campaign_False_br` instead of `campaign_no_br`). Same risk applies to
any future market code that collides with a YAML boolean-like token
(`yes`, `on`, `off`, `true`, `false`).

### `policy_tags`

BigQuery Data Catalog policy-tag taxonomy metadata, used for GDPR/PII
column-level security:

- `taxonomy_project` / `taxonomy_dataset` / `metadata_table` / `taxonomy_table`
  / `taxonomy_location` — where the taxonomy metadata lives.
- `taxonomy_ids` — the taxonomy IDs to scan (currently
  `gdpr_compliance_measures_high/medium/low`).
- `flatten_prefixes` — prefixes stripped from a column name before matching
  it against a taxonomy `display_name` (e.g. `raw_properties_city` ->
  `city`). Only relevant to the automatic fallback (see below); explicit
  `pii_fields` entries always use the real, prefixed column name directly.

### Policy tags

Policy tags are applied directly against the live table schema in BigQuery
Data Catalog, for GDPR/PII column-level security — not via a dbt
`schema.yml`, so tagging takes effect immediately with no dependency on a
separate dbt run.

Two resolution paths, tried in this priority order (per table, via the
`pii_fields` / `apply_policy_tags` config keys — see `tables` below):

1. **Explicit `pii_fields` config** (a `{column_name: value}` dict) — if a
   table defines this, ONLY these exact columns get tagged, nothing else is
   scanned. Each value is either a full policy tag resource path (used
   directly, no taxonomy query needed) or a plain taxonomy `display_name`
   (resolved via the taxonomy metadata tables).
2. **Automatic whole-schema name matching** — the fallback for a table with
   NO `pii_fields` defined at all: every column name is normalized
   (`flatten_prefixes` stripped first) and checked against the taxonomy's
   `display_names`. Best-effort coverage for a table nobody's curated yet —
   it CAN produce a false positive (e.g. `raw_properties_comment` matching
   the taxonomy's generic `comment` entry). Once you've reviewed a table's
   real PII columns, add an explicit `pii_fields` entry — that immediately
   takes over.

### `tables`

Per source table. Options:

| Key | Required | Meaning |
|---|---|---|
| `source_table` | no | Real table name in `gcloudltds`, if it differs from the config key. |
| `primary_key` | no (default `row_hash`) | Passed straight to `bq_ingestor`. |
| `cluster_fields` | no (default `["row_hash"]`) | Passed straight to `bq_ingestor`. |
| `incremental_field` | no | A timestamp column to window reads by (e.g. `ingest_timestamp`). Omit entirely for a snapshot/dimension table that needs a full pull every run — there's no timestamp to filter by. |
| `infer_deleted_from_full_load` | no (default `false`) | Only meaningful for a table with **no** `incremental_field` (a full pull every run) — flags rows missing from the fresh pull as deleted. Do **not** combine with `dedupe_raw_writes`; see below. |
| `flatten_fields` | no | List of top-level struct fields to promote every sub-field of into its own top-level column (e.g. `["raw_properties"]`). |
| `drop_fields` | no | List of top-level fields to drop entirely (e.g. to scrap `properties` once `raw_properties` is flattened instead). |
| `dedupe_raw_writes` | no (default `false`) | For a source with **no** `incremental_field` at all (e.g. `customers_external_id`, which has no timestamp column): every run re-fetches the entire source table. Without this, the raw write would re-append that full dataset every run (`bq_ingestor`'s raw write has no dedup of its own). With it, only new-or-changed rows (by `row_hash`) are written to raw, and deletions are detected and flagged ourselves (rows in the final table whose hash is missing from the fresh pull) instead of relying on `bq_ingestor`'s own `infer_deleted_from_full_load` — that mechanism assumes the merge's "source" is a complete snapshot, which no longer holds once raw only gets the delta. |
| `apply_policy_tags` | no (default `false`) | Whether to apply BigQuery Data Catalog policy tags to this table's final schema (idempotent — only pushes a schema update if something's actually missing a tag). |
| `pii_fields` | no | `{column_name: policy_tag_resource_path_or_display_name}` — tags **only** these exact columns, nothing else scanned. If omitted (and `apply_policy_tags: true`), falls back to scanning every column and tagging whatever matches a taxonomy `display_name` by (prefix-stripped, normalized) name — best-effort coverage for a table nobody's curated yet, which can produce false positives (e.g. `raw_properties_comment` matching a generic `comment` taxonomy entry). Once you've reviewed a table's real PII columns, add an explicit `pii_fields` entry — that immediately takes over. |

### Adding a new source table

Add an entry under `tables:` (applies to `dev` and `prod` automatically,
since both alias the same `&tables` anchor):

```yaml
my_new_table:
  source_table: real_name_in_gcloudltds   # only needed if it differs from this key
  primary_key: row_hash                   # or a real column/list if one exists —
                                           # profile_source_tables.py can help check
  cluster_fields: ["row_hash"]
  incremental_field: ingest_timestamp     # only if the table is a growing event
                                           # log like campaign; omit entirely for
                                           # snapshot/dimension tables (full pull
                                           # each run)
  infer_deleted_from_full_load: true      # true only if the table gets a full
                                           # pull (no incremental_field) and you
                                           # want removed source rows flagged
                                           # is_deleted — don't combine with
                                           # dedupe_raw_writes
```

### Run parameters

`env`, `backfill_start`, `backfill_end`, and `is_backfill` are the flow's
only Prefect-level parameters — the ones settable per run from the UI's
"Custom Run" dialog or via `prefect deployment run ... -p name=value`.
Everything else (markets, tables, policy tags, `dedupe_raw_writes`, etc.)
only lives in `config.yml` and requires an edit to that file to change.

- `backfill_start` / `backfill_end` — ISO-8601 timestamps that override the
  normal rolling daily window for any table with an `incremental_field`
  configured (currently just `campaign`). Tables without one (e.g.
  `customers_external_id`) always do a full pull regardless.
- `is_backfill` (bool, default `false`) — a different, more drastic thing
  than `backfill_start`/`backfill_end`: when true, it ignores
  `incremental_field` entirely for **every** table (a full pull, no
  windowing at all), rather than just moving the window bounds. Despite
  the similar name, it does not use `backfill_start`/`backfill_end`.

## Other scripts in this directory

- `test_connection.py` — standalone connectivity check.
- `profile_source_tables.py` — schema dump, id-column uniqueness check, and
  sample rows for a source table; useful when adding a new table or picking
  a `primary_key`.
