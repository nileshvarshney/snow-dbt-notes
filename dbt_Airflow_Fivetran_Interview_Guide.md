# Supplement: dbt + Airflow (Orchestration) + Fivetran (Ingestion) on Snowflake

> **Companion to:** *Senior_Snowflake_Data_Engineer_Interview_Guide.md* (see section 14 there for core dbt).
> This file covers **how dbt works in a real stack**: Fivetran lands data into Snowflake, dbt transforms it, and Airflow orchestrates the whole flow.
>
> 🟢 basic · 🟡 intermediate · 🔴 senior differentiator
>
> Tool versions move quickly (Airflow 3.x, dbt 1.9/1.10 and the Fusion engine, Fivetran API changes). The facts here are accurate as of mid-2026. Confirm version-specific details against the official docs.

---

## Table of Contents

1. [The Big Picture: How the Three Fit Together](#1-the-big-picture)
2. [Fivetran Essentials](#2-fivetran-essentials)
3. [Fivetran → Snowflake Setup & Best Practices](#3-fivetran--snowflake-setup)
4. [Building dbt Models on Top of Fivetran Data](#4-building-dbt-models-on-top-of-fivetran-data)
5. [Fivetran Q&A](#5-fivetran-qa)
6. [Airflow Essentials for Data Engineers](#6-airflow-essentials)
7. [Running dbt from Airflow: 4 Patterns](#7-running-dbt-from-airflow)
8. [Astronomer Cosmos Deep Dive](#8-astronomer-cosmos-deep-dive)
9. [End-to-End DAG: Fivetran → dbt → Alerts](#9-end-to-end-dag)
10. [Backfills, Reruns, and Idempotency](#10-backfills-reruns-idempotency)
11. [Airflow + dbt Q&A](#11-airflow--dbt-qa)
12. [Monitoring, Alerting & Observability Across the Stack](#12-monitoring-alerting--observability)
13. [Cost Optimization Across the Stack](#13-cost-optimization)
14. [Troubleshooting Scenarios](#14-troubleshooting-scenarios)
15. [Senior Design Questions](#15-senior-design-questions)
16. [Cheat Sheet](#16-cheat-sheet)

---

## 1. The Big Picture

```
 SaaS apps (Salesforce, HubSpot, Stripe, NetSuite…)  ┐
 Databases (Postgres, MySQL, SQL Server, Oracle) CDC  ├──► FIVETRAN ──► Snowflake RAW database
 Files / events (S3, SFTP, webhooks)                   ┘      (EL)        (one schema per connection)
                                                                              │
                                                     AIRFLOW (orchestrates)   │  trigger / sense sync done
                                                         │                    ▼
                                                         └──────────►  dbt (T): staging → intermediate → marts
                                                                              │   tests, snapshots, docs
                                                                              ▼
                                                                  BI / reverse ETL / data products
```

**Who does what**

| Layer | Tool | Responsibility |
|---|---|---|
| Extract + Load | **Fivetran** | Managed connectors, incremental sync / CDC, schema drift handling, landing raw tables in Snowflake |
| Transform | **dbt** | Business logic, modeling (Kimball), tests, documentation, lineage |
| Orchestrate | **Airflow** | Scheduling, dependencies across tools (Fivetran → dbt → downstream), retries, alerting, backfills |
| Store/Compute | **Snowflake** | Storage, compute for Fivetran's merges and dbt's SQL, governance |

**Key interview message:** each tool does one job. Fivetran **should not** do business transformations. dbt **should not** do extraction. Airflow **should not** process data itself (it orchestrates and passes metadata, not data).

> **Industry context:** Fivetran and dbt Labs announced a merger in late 2025. Expect questions on the combined direction (tighter Fivetran ↔ dbt integration), but the fundamentals here still apply. Verify the current status before you mention it.

---

## 2. Fivetran Essentials

### Core concepts

- **Connection** (formerly called a *connector* in the UI and API): one configured source. Each connection writes to **its own schema** in the destination (e.g., `RAW.SALESFORCE`, `RAW.POSTGRES_ORDERS_PUBLIC`).
- **Destination**: e.g., your Snowflake account and database.
- **Sync**: each run pulls changes since the last cursor/state. The **initial (historical) sync** pulls all history, and later syncs are **incremental**.
- **Sync frequency**: configurable from 24 hours down to 5 minutes (1-minute syncs on higher plans). The default is commonly 6 hours.
- **Re-sync**: full historical reload of a table or connection (after corruption, a schema mismatch, or a cursor problem). Expensive in time and Snowflake compute, and it can break dbt incremental assumptions (see section 4).
- **Pricing**: based on **MAR (Monthly Active Rows)**, the distinct primary keys inserted, updated, or deleted per month (counted per connection under recent pricing). Initial historical syncs generally don't count. Check current pricing for re-syncs and edge cases.

### Database connectors & CDC

- **Log-based CDC** for databases: MySQL binlog, Postgres logical replication (WAL, replication slots), SQL Server change tracking/CDC, Oracle (LogMiner / binary log reader / HVR). It captures inserts, updates, **and deletes** with low source impact.
- **Teleport Sync**: a snapshot-comparison method for sources where logs aren't available.
- **Postgres gotcha:** an inactive or failing connector can make the **replication slot retain WAL** and fill the source disk. Monitor slot lag.

### Fivetran system columns (must know!)

| Column | Meaning | How you use it in dbt |
|---|---|---|
| `_FIVETRAN_SYNCED` | Timestamp when Fivetran last wrote the row | `loaded_at_field` for **source freshness**. Incremental watermark |
| `_FIVETRAN_DELETED` | Boolean **soft delete** flag (the row was deleted in the source) | Filter out in staging, or propagate as `is_deleted` |
| `_FIVETRAN_ID` | Synthetic key for tables **without a primary key** | Use as a unique key when there's no natural PK |
| `_FIVETRAN_START`, `_FIVETRAN_END`, `_FIVETRAN_ACTIVE` | **History mode** columns (SCD2 maintained by Fivetran) | Build SCD2 dims directly, often instead of dbt snapshots |
| `_FIVETRAN_INDEX`, `_FIVETRAN_FILE`, `_FIVETRAN_LINE`, `_FIVETRAN_MODIFIED` | File connector metadata (S3/SFTP) | Lineage, dedupe |

> Some connectors **hard delete** or don't capture deletes at all (many SaaS APIs only expose updates). Know per source whether deletes arrive.

### Schema change handling

Per connection, choose how new schemas, tables, and columns are handled:
- **Allow all**: new schemas, tables, and columns sync automatically
- **Allow columns**: new columns sync, new tables are blocked until approved
- **Block all**: nothing new syncs until approved

Fivetran also handles **type changes** by widening (e.g., INT → FLOAT → STRING), and it doesn't drop removed columns. They stop updating (values become NULL for new rows).

> **Senior take:** "Allow columns" for production sources that feed contracted models, with alerts on schema change events, plus dbt contracts downstream. That way drift is visible but doesn't break ingestion.

### Fivetran transformations & dbt packages

- **Fivetran Transformations for dbt Core**: Fivetran can run your dbt project (from your Git repo) on a schedule, or with **integrated scheduling**, which starts the models right after their source connections finish syncing.
- **Quickstart data models**: prebuilt dbt models Fivetran runs for common sources.
- **Fivetran dbt packages** (open source on dbt Hub): `fivetran/salesforce`, `fivetran/hubspot`, `fivetran/stripe`, `fivetran/netsuite`, `fivetran/fivetran_log`, and so on. They provide staging and modeled tables for each source schema. Common to install and configure with `vars` (e.g., schema name, enabling or disabling sub-objects).
- **Fivetran Platform connector** (`fivetran_log`): syncs Fivetran's own metadata (sync logs, MAR usage, schema changes) into Snowflake. Great for monitoring and cost dashboards.

### Fivetran REST API (for orchestration)

- Trigger a sync, check status, pause and resume, re-sync, change schema configs.
- Core idea: Airflow calls the API to **trigger a sync**, then **polls/defers** until the sync finishes (`succeeded_at` / `failed_at`), then runs dbt.
- Auth: API key + secret (store in Airflow Connections / a secrets backend).
- Note: the API resource naming has moved from `connectors` to `connections`. Use the current endpoints in the docs.

---

## 3. Fivetran → Snowflake Setup

### Recommended Snowflake objects

```sql
-- Dedicated role, user, warehouse, database for Fivetran
USE ROLE SECURITYADMIN;
CREATE ROLE FIVETRAN_ROLE;
CREATE USER FIVETRAN_USER TYPE = SERVICE DEFAULT_ROLE = FIVETRAN_ROLE DEFAULT_WAREHOUSE = FIVETRAN_WH
  RSA_PUBLIC_KEY = '<public key>';                -- key-pair auth (no passwords)
GRANT ROLE FIVETRAN_ROLE TO USER FIVETRAN_USER;
GRANT ROLE FIVETRAN_ROLE TO ROLE SYSADMIN;        -- roll up to SYSADMIN

USE ROLE SYSADMIN;
CREATE WAREHOUSE FIVETRAN_WH WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
CREATE DATABASE RAW;
GRANT USAGE ON WAREHOUSE FIVETRAN_WH TO ROLE FIVETRAN_ROLE;
GRANT USAGE, CREATE SCHEMA, MONITOR ON DATABASE RAW TO ROLE FIVETRAN_ROLE;  -- Fivetran creates one schema per connection

-- dbt reads RAW, writes ANALYTICS
GRANT USAGE ON DATABASE RAW TO ROLE DBT_TRANSFORMER;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE RAW TO ROLE DBT_TRANSFORMER;
GRANT SELECT ON FUTURE TABLES IN DATABASE RAW TO ROLE DBT_TRANSFORMER;
```
> Fivetran owns the schemas it creates. Grant `FIVETRAN_ROLE` only on RAW. dbt's role gets **read-only** access to RAW. Add a **resource monitor** on `FIVETRAN_WH`.

### How Fivetran writes into Snowflake

- It stages data in its own cloud storage (or yours), then `COPY` → `MERGE` into destination tables by primary key. These merges run on **your Snowflake warehouse**, so you pay **Fivetran MAR + Snowflake credits**.
- High-frequency syncs (every 5–15 min) on big tables keep the warehouse awake constantly. Balance freshness against credits.
- Fivetran declares **primary keys** on destination tables (informational in Snowflake).
- Column names are typically lower/snake-cased and stored uppercase in Snowflake (unquoted identifiers).
- Network: allowlist Fivetran IPs in a Snowflake network policy, or use PrivateLink on higher tiers.

### Best practices

1. One RAW database, one schema per connection, and never write to RAW from anything else.
2. Dedicated XS warehouse for Fivetran with auto-suspend at 60s and a resource monitor.
3. Key-pair auth, least privilege, a network policy.
4. Sync frequency set by business SLA, not "as fast as possible."
5. Deselect unused tables and columns (saves MAR and credits, reduces PII exposure). **Column blocking/hashing** for PII at ingestion.
6. Turn on **history mode** only where SCD2 is actually needed (it increases rows and MAR).
7. Sync the **Fivetran Platform connector** for monitoring.
8. Manage connections as code where possible (Fivetran **Terraform provider** / API) for repeatable environments.

---

## 4. Building dbt Models on Top of Fivetran Data

### 4.1 Declare sources with freshness on `_fivetran_synced`

```yaml
# models/staging/salesforce/_salesforce__sources.yml
version: 2
sources:
  - name: salesforce
    database: raw
    schema: salesforce
    loaded_at_field: _fivetran_synced
    freshness:
      warn_after:  {count: 12, period: hour}
      error_after: {count: 24, period: hour}
    tables:
      - name: account
        columns:
          - name: id
            data_tests: [unique, not_null]
      - name: opportunity
      - name: user
        freshness: null         # opt out for slow-changing table
```
> Set the freshness thresholds a bit looser than the sync frequency (e.g., sync every 6h → warn at 12h). Run `dbt source freshness` at the start of the DAG. In dbt 1.9+, `loaded_at_field` and freshness can also live under `config:`.

### 4.2 Staging pattern for Fivetran tables

```sql
-- models/staging/salesforce/stg_salesforce__accounts.sql
with source as (
    select * from {{ source('salesforce', 'account') }}
),
renamed as (
    select
        id                              as account_id,
        name                            as account_name,
        owner_id,
        industry,
        cast(annual_revenue as number(18,2)) as annual_revenue,
        convert_timezone('UTC', created_date)::timestamp_ntz  as created_at,
        convert_timezone('UTC', last_modified_date)::timestamp_ntz as updated_at,
        coalesce(_fivetran_deleted, false)                    as is_deleted,
        _fivetran_synced                                      as _loaded_at
    from source
)
select * from renamed
where not is_deleted     -- or keep and propagate, depending on downstream needs
```

**Staging rules for Fivetran sources**
- 1:1 with source tables, materialized as **views**.
- Rename to business-friendly snake_case, cast types, standardize timezones (UTC).
- Handle `_fivetran_deleted`: **filter it** in staging for most marts, or **keep it as `is_deleted`** when downstream needs deletion history or incremental models must delete rows.
- Carry `_fivetran_synced` through as `_loaded_at` for incremental logic and debugging.
- SaaS APIs can resend the same row, but Fivetran merges on the PK, so duplicates are rare. For tables using `_fivetran_id` or files, **dedupe** in staging.

### 4.3 Incremental models using `_fivetran_synced`

```sql
-- models/marts/sales/fct_opportunities.sql
{{ config(
    materialized = 'incremental',
    unique_key = 'opportunity_id',
    incremental_strategy = 'merge',
    on_schema_change = 'append_new_columns'
) }}

select
    opportunity_id, account_id, stage_name, amount, close_date, is_deleted, _loaded_at
from {{ ref('stg_salesforce__opportunities') }}   -- staging keeps is_deleted for this model

{% if is_incremental() %}
where _loaded_at > (select dateadd(hour, -3, max(_loaded_at)) from {{ this }})   -- lookback buffer
{% endif %}
```
**Why `_fivetran_synced` and not the source's `updated_at`?**
- `_fivetran_synced` reflects **when the data arrived in Snowflake**, so it catches late-arriving updates and deletes (soft-deleted rows get a new `_fivetran_synced`).
- The source's `updated_at` can be unreliable, missing, or in another timezone, and some deletes don't change it.
- Use the business timestamp for **placement** (partitioning, clustering), and `_fivetran_synced` for **detection**.

**Handling soft deletes in an incremental model**
- Option A: keep `is_deleted` as a column in the fact (simplest, auditable). BI filters `where not is_deleted`.
- Option B: delete in a post-hook:
  ```sql
  post_hook = "delete from {{ this }} where is_deleted"
  ```
- Option C: custom merge logic (macro) with `when matched and is_deleted then delete`.

### 4.4 ⚠️ Re-syncs and incremental models (classic senior question)

When Fivetran **re-syncs** a table, **every row gets a new `_fivetran_synced`**. Effects:
- The next incremental run picks up the **entire table** as "new." That's not wrong when MERGE is idempotent, but it's slow and expensive. With `append` strategy it **creates duplicates**.
- A re-sync can also **lose soft-delete history** in some cases (deleted rows may not come back flagged), so marts can end up with "ghost" rows.

**Mitigations**
- Use `merge` (idempotent), not `append`, on Fivetran-sourced incremental models.
- Monitor re-sync events (Fivetran Platform connector logs / webhooks) and run a planned `--full-refresh` of affected models in a controlled window.
- Periodic **reconciliation**: compare keys in staging vs the mart and remove orphans.
- Use `incremental_predicates` to limit the target scan during the big merge.

### 4.5 History mode vs dbt snapshots

| | Fivetran **history mode** | dbt **snapshots** |
|---|---|---|
| Captures | **Every change** Fivetran sees (each sync/CDC event) | Only changes visible **at snapshot run time** (misses intermediate changes between runs) |
| Where | Ingestion (Fivetran maintains `_fivetran_start/_end/_active`) | Transformation (dbt `dbt_valid_from/to`) |
| Cost | More MAR and rows | Snowflake compute per run |
| Backfill | Only from when it was enabled | Only from when you started snapshotting |
| Best for | Audit-grade, full-fidelity history from CDC sources | Sources without history mode, or SCD2 on curated data |

> In an SCD2 dimension built from history mode, use `_fivetran_start` as `valid_from`, `_fivetran_end` as `valid_to`, and `_fivetran_active` as `is_current`. Dedupe versions that differ only in columns you don't track.

### 4.6 Using Fivetran dbt packages

```yaml
# packages.yml
packages:
  - package: fivetran/salesforce
    version: [">=1.0.0", "<2.0.0"]     # pin a range; check dbt Hub for current versions

# dbt_project.yml
vars:
  salesforce_schema: salesforce        # where Fivetran landed it
  salesforce__user_role_enabled: false # disable models for tables you didn't sync
models:
  salesforce:
    +schema: salesforce_modeled
    +materialized: table
```
**Pros:** fast start, maintained by Fivetran, handles connector quirks. **Cons:** generic modeling, possible conflicts with your naming conventions, version upgrades can change outputs. Typical senior approach: use the package's **staging/source** models, and build your own marts on top. Pin versions and test upgrades in CI.

### 4.7 Multiple instances of the same source (e.g., 3 Shopify stores)

Each connection lands in a separate schema. Union them in staging:
```sql
{{ dbt_utils.union_relations(
     relations=[source('shopify_us','orders'), source('shopify_eu','orders'), source('shopify_apac','orders')],
     source_column_name='_dbt_source_relation'
) }}
```
Fivetran packages also support this through a `*_union_schemas` / `*_sources` variable.

---

## 5. Fivetran Q&A

**🟢 Q1. What is Fivetran and why use it instead of building custom pipelines?**
> A managed ELT (extract and load) service with hundreds of prebuilt connectors. It handles API changes, pagination, rate limits, incremental cursors, CDC, schema drift, and retries, and lands normalized raw tables in Snowflake. It saves engineering time on undifferentiated work so the team can focus on modeling and business logic. Trade-offs: cost (MAR-based), less control over extraction details, and some connectors' quirks. For high-volume event data or unusual sources, custom pipelines (Snowpipe, Kafka) or other tools can be cheaper.

**🟢 Q2. What are `_fivetran_synced` and `_fivetran_deleted`?**
> `_fivetran_synced` is when Fivetran last wrote the row to Snowflake. I use it for source freshness and as the incremental watermark. `_fivetran_deleted` is a soft-delete flag set when the row was deleted in the source. I filter or propagate it in staging.

**🟡 Q3. How does Fivetran handle deletes?**
> For CDC database connectors, deletes are captured from the logs and rows are **soft deleted** (`_fivetran_deleted = TRUE`) rather than removed. Many SaaS connectors only capture deletes if the API exposes them. Some capture them through periodic re-imports, and some not at all. So in dbt I must explicitly handle `_fivetran_deleted`, and for sources without delete capture, reconcile periodically.

**🟡 Q4. What happens downstream when a source adds, removes, or renames a column?**
> Depending on the schema change setting, Fivetran adds the new column automatically. Removed columns remain but stop populating. A rename looks like a new column plus a stale old one. In dbt: `select *` in staging is risky, so I list columns explicitly. Incremental models use `on_schema_change` (`append_new_columns`), contracts on published models make breaking changes fail CI, and I monitor schema-change events from the Fivetran log connector.

**🟡 Q5. How do you control Fivetran costs?**
> Deselect unneeded tables and columns, choose sync frequency based on the SLA, avoid history mode unless needed, watch MAR by connection and table using the Fivetran Platform connector, avoid unnecessary re-syncs, and on the Snowflake side use an XS warehouse with auto-suspend and a resource monitor. Also check for "chatty" tables (e.g., a table where an `updated_at` timestamp changes every sync), which inflate MAR.

**🟡 Q6. What is a re-sync and what are the risks?**
> A full historical reload of a table or connection, used to fix data issues or after certain schema changes. Risks: long runtimes, source load, Snowflake credits for big merges, all rows getting a new `_fivetran_synced` (so incremental models reprocess everything, and `append` models duplicate), and potential loss of soft-delete history. I schedule it deliberately and follow up with targeted full refreshes and reconciliation.

**🔴 Q7. How do you make sure dbt doesn't run on partially synced data?**
> Orchestrate explicitly: Airflow triggers the Fivetran sync and waits for completion (`FivetranOperator` + sensor), then runs dbt for downstream models only. Or use Fivetran's integrated scheduling, or data-aware scheduling (Airflow Assets). Also run `dbt source freshness` as a gate. Fivetran commits per table, so cross-table consistency within a connection isn't guaranteed mid-sync. Waiting for "sync succeeded" is the right boundary.

**🔴 Q8. Postgres CDC connector keeps failing and the source DB's disk is filling up. What's happening?**
> The logical replication slot is retaining WAL because Fivetran isn't consuming it (connector paused or failing, or there's a long-running transaction). Check slot lag in `pg_replication_slots`, fix the connector error, and if necessary drop and recreate the slot (which needs a re-sync). Prevent it with alerts on slot lag and connector failures, and with `max_slot_wal_keep_size` on the source.

**🔴 Q9. Fivetran history mode or dbt snapshots for SCD2?**
> History mode when I need **every** change with audit fidelity from a CDC source. It captures changes between dbt runs that snapshots would miss. dbt snapshots when history mode isn't available or affordable, or when I'm tracking changes in a derived dataset. Either way, I start capturing history early, since neither can recreate the past.

---

## 6. Airflow Essentials

### Core concepts

- **DAG**: a Python-defined directed acyclic graph of tasks with a schedule.
- **Task / Operator**: a unit of work (`BashOperator`, `PythonOperator`, `SnowflakeSqlApiOperator` / `SQLExecuteQueryOperator`, `KubernetesPodOperator`, provider operators).
- **Sensor**: waits for a condition (file exists, external task done, API status). Use **deferrable** mode (`deferrable=True`) so it doesn't hold a worker slot while waiting (it runs on the **triggerer**).
- **TaskFlow API**: `@dag`, `@task` decorators. Return values passed via **XCom** (small metadata only, never datasets).
- **Scheduler, executor (Local/Celery/Kubernetes), workers, triggerer, metadata DB, web server/API server.**
- **Scheduling**: cron or timedelta `schedule`, **data intervals** (`data_interval_start`/`end`, `logical_date`, `{{ ds }}`), `catchup`, `start_date`, `max_active_runs`.
- **Data-aware scheduling**: **Assets** (Airflow 3, called *Datasets* in Airflow 2.4+). A DAG runs when upstream tasks update an asset. Good for "run dbt when Fivetran-landed data is updated."
- **Retries**: `retries`, `retry_delay`, `retry_exponential_backoff`, `execution_timeout`.
- **Pools**: limit concurrency to a shared resource (e.g., a `snowflake` pool with 8 slots).
- **Callbacks**: `on_failure_callback`, `on_success_callback`, `on_retry_callback` (Slack, PagerDuty, email).
- **Connections & Variables**: stored in a **secrets backend** (AWS Secrets Manager, Vault, GCP Secret Manager), never hardcoded.
- **Trigger rules**: `all_success` (default), `all_done`, `one_failed`, `none_failed`, etc. (e.g., an alert or cleanup task that runs even if upstream fails).
- **Task groups**: visual and logical grouping (SubDAGs are removed in Airflow 3).
- **Dynamic task mapping**: `.expand()` to fan out over a list at runtime (e.g., one task per Fivetran connection).

**Airflow 3 changes worth knowing (headline level):** new React UI, **DAG versioning**, Datasets renamed to **Assets** (`@asset` decorator), a Task SDK and task execution API (workers don't talk directly to the metadata DB), removal of SubDAGs and some legacy context variables (`execution_date` → `logical_date`), `catchup` defaulting to **False**, and event-driven scheduling. Many companies still run Airflow 2.x (or MWAA, Cloud Composer, Astronomer), so know both and say which one you've used.

### Airflow best practices (interview list)
1. **Idempotent, atomic tasks**. Re-running a task for the same interval gives the same result.
2. Use the **data interval** (`{{ data_interval_start }}`/`{{ ds }}`), not `datetime.now()`, so backfills work.
3. **Airflow orchestrates, it doesn't compute**: push work to Snowflake, dbt, Fivetran. Keep workers light.
4. No heavy top-level code in DAG files (it slows the scheduler's parsing).
5. Deferrable operators and sensors for long waits.
6. Secrets in a backend. Connections per environment.
7. Pools and `max_active_runs` to protect Snowflake and the sources.
8. Alerts on failure and on SLA/deadline misses, with clear ownership (tags, `owner`).
9. DAGs in Git with CI (DAG import tests, linting, unit tests for custom operators).
10. Separate DAGs by cadence and domain rather than one giant DAG.

---

## 7. Running dbt from Airflow

### Pattern comparison

| Pattern | How | Pros | Cons | When |
|---|---|---|---|---|
| **1. BashOperator `dbt build`** | One task runs the whole project or selection | Simplest | One giant task: no per-model visibility, a failure means rerunning everything (mitigate with `dbt retry`), dbt must be installed on workers | Small projects, quick start |
| **2. Astronomer Cosmos** | Parses the dbt project and renders **each model (+ its tests) as an Airflow task** in a DAG or TaskGroup | Per-model retries, logs, and lineage in Airflow. Rerun just the failed model | Big projects mean many tasks (scheduler load). Parsing overhead (use manifest load mode) | Most popular for dbt Core + Airflow |
| **3. dbt Cloud / dbt platform** | `DbtCloudRunJobOperator` triggers a dbt Cloud job. A sensor waits | dbt Cloud handles environments, Slim CI, IDE, docs, Explorer. Airflow stays light | dbt Cloud licensing. Less granular in Airflow | Teams on dbt Cloud wanting cross-tool orchestration |
| **4. Containerized** (`KubernetesPodOperator` / ECS / Docker) | Run a versioned dbt Docker image per task | Dependency isolation, reproducible, scales independently of workers | More infrastructure, image build pipeline | Larger platforms, many teams/projects |

> **Dependency conflicts:** dbt and Airflow both pin many Python packages. Install dbt in a **separate virtualenv** (Cosmos supports `ExecutionMode.VIRTUALENV` and a `dbt_executable_path`) or use containers.

### Pattern 1: BashOperator (with best practices)
```python
from airflow.operators.bash import BashOperator   # Airflow 3: airflow.providers.standard.operators.bash

dbt_build = BashOperator(
    task_id="dbt_build_marts",
    bash_command=(
        "cd /opt/dbt/analytics && "
        "dbt deps && "
        "dbt build --target prod --select tag:daily "
        "--vars '{\"run_date\": \"{{ ds }}\"}'"
    ),
    env={"DBT_PROFILES_DIR": "/opt/dbt/profiles",
         "SNOWFLAKE_PRIVATE_KEY_PATH": "/secrets/dbt_key.p8"},   # injected from secrets backend
    append_env=True,
    retries=1,
)
```
- Use `dbt retry` on the retry attempt (reruns only failed and skipped nodes, based on `run_results.json`), or `--select result:error+ --state target/`.
- Push `target/run_results.json` and `manifest.json` to S3 for monitoring and Slim CI.

### Pattern 3: dbt Cloud provider
```python
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

run_dbt_cloud = DbtCloudRunJobOperator(
    task_id="run_dbt_cloud_daily",
    dbt_cloud_conn_id="dbt_cloud",
    job_id=12345,
    steps_override=["dbt build --select tag:daily"],
    wait_for_termination=True,
    deferrable=True,
    timeout=3600,
)
```

---

## 8. Astronomer Cosmos Deep Dive

```python
from datetime import datetime
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import LoadMode, TestBehavior
from cosmos.profiles import SnowflakePrivateKeyPemProfileMapping

profile_config = ProfileConfig(
    profile_name="analytics",
    target_name="prod",
    profile_mapping=SnowflakePrivateKeyPemProfileMapping(   # builds profiles.yml from an Airflow connection
        conn_id="snowflake_dbt",
        profile_args={"database": "ANALYTICS", "schema": "MARTS", "warehouse": "TRANSFORM_WH", "role": "DBT_TRANSFORMER"},
    ),
)

marts = DbtTaskGroup(
    group_id="dbt_sales_marts",
    project_config=ProjectConfig(
        dbt_project_path="/opt/dbt/analytics",
        manifest_path="/opt/dbt/analytics/target/manifest.json",  # precompiled in CI → fast parsing
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path="/opt/dbt_venv/bin/dbt"),  # isolated venv
    render_config=RenderConfig(
        load_method=LoadMode.DBT_MANIFEST,
        select=["source:salesforce+"],          # only models downstream of the Salesforce source
        test_behavior=TestBehavior.AFTER_EACH,  # run each model's tests right after it (default)
    ),
    operator_args={"install_deps": True, "vars": {"run_date": "{{ ds }}"}},
    default_args={"retries": 2, "pool": "snowflake"},
)
```

**Key Cosmos concepts**
- **`DbtDag`** (the whole DAG is dbt) vs **`DbtTaskGroup`** (dbt inside a larger DAG with Fivetran tasks, etc.).
- **Load modes**: `DBT_LS` (runs `dbt ls` at parse time, accurate but slower), `DBT_MANIFEST` (reads a precompiled manifest, fastest. Generate it in CI/CD), `CUSTOM` (Cosmos parser), `AUTOMATIC`.
- **Execution modes**: `LOCAL` (same environment), `VIRTUALENV`, `DOCKER`, `KUBERNETES`, plus cloud container options and `AIRFLOW_ASYNC` (deferrable, for some adapters).
- **Test behavior**: `AFTER_EACH` (model then its tests, so failures stop downstream), `BUILD` (uses `dbt build` per node), `AFTER_ALL`, `NONE`.
- **Selection**: `select`/`exclude` with dbt syntax (`tag:`, `path:`, `+model`, `source:x+`).
- **Sources and freshness**: Cosmos can render source nodes (`source_rendering_behavior`) to run source freshness checks as tasks.
- **Profile mappings**: generate `profiles.yml` from Airflow Connections, so secrets stay in Airflow's secrets backend.

**Scaling concerns:** a 1,500-model project means 3,000 or more tasks, which strains the scheduler. Solutions: split into multiple DbtTaskGroups/DAGs by domain or tag, use manifest load mode, group related models with `dbt build` per folder instead of per model, or use containerized execution.

---

## 9. End-to-End DAG

**Goal:** every day at 05:00 UTC, sync Salesforce and HubSpot via Fivetran, wait for both, check freshness, build the dbt marts, alert on failure, and publish an asset so downstream DAGs (reverse ETL, ML features) start automatically.

```python
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.datasets import Dataset          # Airflow 3: from airflow.sdk import Asset
from airflow.operators.bash import BashOperator
from fivetran_provider_async.operators import FivetranOperator
from cosmos import DbtTaskGroup, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode
from include.alerts import slack_failure_callback          # your callback
from include.dbt_config import profile_config, execution_config, DBT_PATH

SALES_MARTS = Dataset("snowflake://ANALYTICS/MARTS/sales")

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": slack_failure_callback,
    "execution_timeout": timedelta(hours=2),
}

@dag(
    dag_id="sales_elt_daily",
    schedule="0 5 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["sales", "fivetran", "dbt"],
)
def sales_elt_daily():

    # 1) Extract + Load: trigger Fivetran syncs and wait (deferrable = no worker slot held)
    sync_salesforce = FivetranOperator(
        task_id="sync_salesforce",
        fivetran_conn_id="fivetran_default",
        connector_id="{{ var.value.fivetran_salesforce_id }}",
        wait_for_completion=True,
        deferrable=True,
    )
    sync_hubspot = FivetranOperator(
        task_id="sync_hubspot",
        fivetran_conn_id="fivetran_default",
        connector_id="{{ var.value.fivetran_hubspot_id }}",
        wait_for_completion=True,
        deferrable=True,
    )

    # 2) Gate: source freshness (fails if data is stale even though sync "succeeded")
    freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_PATH} && dbt source freshness --select source:salesforce source:hubspot --target prod",
    )

    # 3) Transform: one Airflow task per model + tests
    dbt_marts = DbtTaskGroup(
        group_id="dbt_sales",
        project_config=ProjectConfig(DBT_PATH, manifest_path=f"{DBT_PATH}/target/manifest.json"),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST,
                                   select=["source:salesforce+", "source:hubspot+"]),
        default_args={"pool": "snowflake"},
    )

    # 4) Publish asset → triggers downstream DAGs scheduled on SALES_MARTS
    @task(outlets=[SALES_MARTS])
    def publish():
        return "sales marts refreshed"

    [sync_salesforce, sync_hubspot] >> freshness >> dbt_marts >> publish()

sales_elt_daily()
```

**Talking points for this DAG**
- **Deferrable** Fivetran operators don't hold workers during long syncs.
- **Fan-in**: dbt waits for **both** syncs.
- The **freshness gate** catches a "successful" sync that brought nothing (paused table, API issue).
- **Per-model tasks** (Cosmos): a failed model retries alone, and downstream models are skipped (no bad data propagation).
- **Pool** limits concurrent Snowflake queries. `max_active_runs=1` prevents overlapping runs.
- An **asset/dataset outlet** decouples downstream consumers (data-aware scheduling).
- Alternatives: dynamic task mapping over a list of connection IDs (`FivetranOperator.partial(...).expand(connector_id=[...])`), or let Fivetran schedule syncs and have Airflow only **sense** completion (`FivetranSensor`).

> **Fivetran-scheduled vs Airflow-triggered syncs:** with Airflow-triggered syncs, set the connection's schedule to **manual** in Fivetran, so it doesn't also sync on its own schedule and race with dbt.

---

## 10. Backfills, Reruns, and Idempotency

**Make dbt runs interval-aware**
```sql
-- model uses a var with a sensible default
{% set run_date = var('run_date', run_started_at.strftime('%Y-%m-%d')) %}
...
{% if is_incremental() %}
  where event_date between dateadd(day, -2, '{{ run_date }}'::date) and '{{ run_date }}'::date
{% endif %}
```
Airflow passes `--vars '{"run_date": "{{ ds }}"}'`, so rerunning the 2026-11-10 DAG run reprocesses exactly that window.

**dbt microbatch (dbt 1.9+)** is built for this:
```sql
{{ config(materialized='incremental', incremental_strategy='microbatch',
          event_time='event_ts', batch_size='day', begin='2024-01-01', lookback=2) }}
```
- Each batch (day) is processed independently. Failed batches can be retried (`dbt retry`).
- Backfill: `dbt run --select fct_events --event-time-start "2026-10-01" --event-time-end "2026-11-01"`.
- Upstream models with `event_time` configured are filtered automatically per batch.

**Airflow backfill**
- Airflow 2: `airflow dags backfill -s 2026-10-01 -e 2026-10-31 sales_elt_daily`. Airflow 3: backfills can be created from the UI or API and run by the scheduler.
- Consider `max_active_runs` and pools so a backfill doesn't flood Snowflake. Use a separate, bigger warehouse for large backfills (dbt `snowflake_warehouse` config driven by a var).

**Reruns**
- Single-model failure in Cosmos: clear that task. Downstream reruns follow.
- Monolithic BashOperator: use `dbt retry` on retry attempts.
- Fivetran: never "re-sync" just to rerun dbt. Rerun only the dbt part.

**Idempotency checklist:** MERGE or delete+insert (never blind append for mutable data), deterministic surrogate keys, interval-driven filters (not `current_date` in logic), dedupe on the business key, watermarks from target data (`max(_loaded_at)`) rather than wall-clock time.

---

## 11. Airflow + dbt Q&A

**🟢 Q1. Why use Airflow if dbt Cloud (or Fivetran) can schedule dbt?**
> dbt Cloud schedules dbt jobs well, but real pipelines span tools: Fivetran syncs, file arrivals, Snowflake procedures, ML jobs, reverse ETL, API calls, notifications. Airflow orchestrates the **cross-tool dependencies** (dbt runs only after the right syncs succeed), retries, backfills, SLAs, and alerting in one place. If the stack is only Fivetran + dbt, Fivetran's integrated scheduling or dbt Cloud can be enough. Simpler is better.

**🟡 Q2. BashOperator vs Cosmos vs dbt Cloud operator: which do you prefer?**
> For dbt Core I prefer **Cosmos** with manifest load mode and isolated execution. It gives per-model observability and retries, and failures stop downstream models. For very large projects I group by domain or tag to keep task counts manageable, or run containerized `dbt build` per domain. If the team uses dbt Cloud, I use `DbtCloudRunJobOperator` so dbt Cloud keeps environments and CI while Airflow handles cross-tool dependencies. A plain BashOperator is fine for small projects.

**🟡 Q3. How do you handle credentials for dbt in Airflow?**
> Snowflake key-pair credentials are stored in a secrets backend (AWS Secrets Manager, Vault) and surfaced as an Airflow Connection. Cosmos profile mappings build `profiles.yml` from the connection at runtime. With BashOperator, I pass env vars that `profiles.yml` reads with `env_var()`. Never commit profiles with secrets. Use separate service users and roles per environment.

**🟡 Q4. How do you prevent dbt from running before Fivetran finishes?**
> Trigger the sync from Airflow with `FivetranOperator(wait_for_completion=True)`, or use a `FivetranSensor` if Fivetran schedules the sync. Then dbt tasks run downstream. Add a `dbt source freshness` gate. Alternatives: data-aware scheduling (assets), or Fivetran integrated scheduling for dbt.

**🟡 Q5. A dbt model fails in the middle of an Airflow run. What happens and how do you recover?**
> With Cosmos (`AFTER_EACH`), the failed model's task fails, and its downstream tasks are marked `upstream_failed` and skipped. Unrelated branches still complete. I check the task log (compiled SQL and the Snowflake error), fix the issue (code via PR, data upstream), then clear the failed task to rerun it and its downstream. With BashOperator, I use `dbt retry` so only failed and skipped nodes rerun. An alert fires through the failure callback.

**🟡 Q6. How do you structure DAGs for many dbt models with different SLAs?**
> Tag models by cadence and priority (`tag:hourly`, `tag:daily`, `tag:finance_critical`) and build separate DAGs or task groups per tag or domain. Hourly-critical models get their own lightweight DAG. Heavy daily models run overnight on a bigger warehouse (per-model `snowflake_warehouse`). Cross-DAG dependencies use assets/datasets instead of `ExternalTaskSensor` where possible.

**🔴 Q7. How do you keep Airflow, dbt, and Fivetran deployments in sync (CI/CD)?**
> - dbt repo CI: lint, compile, Slim CI, unit tests. On merge, build a **dbt Docker image** or package with a precompiled `manifest.json` and publish it.
> - Airflow repo CI: DAG import tests (no import errors, no cycles), lint, unit tests for callbacks and helpers. Deploy DAGs (Astronomer deploy, MWAA S3 sync, Composer).
> - Cosmos reads the published manifest, so DAG structure updates automatically when models change.
> - Fivetran connections managed with the Terraform provider, so schema settings and sync frequency are versioned.
> - Environment parity: dev, staging, prod Airflow connections pointing to the matching Snowflake databases and roles.

**🔴 Q8. How would you pass lineage and metadata across the stack?**
> dbt artifacts (`manifest.json`, `run_results.json`, `sources.json`) loaded into Snowflake (dbt_artifacts / Elementary). The Fivetran Platform connector for sync and schema events. Airflow OpenLineage provider to emit lineage events. All of it feeds a catalog (DataHub, Atlan, Snowflake Horizon) or a monitoring dashboard. dbt **exposures** document downstream dashboards so impact analysis covers source → dashboard.

**🔴 Q9. Airflow is down for 6 hours. What happens to the pipeline and how do you recover?**
> If Airflow triggers Fivetran, syncs don't run, and data simply waits in the sources (CDC logs keep changes, as long as WAL/binlog retention covers the gap). When Airflow recovers, with `catchup=True` the missed intervals run in order (controlled by `max_active_runs`). With `catchup=False`, the latest run picks up everything, because Fivetran syncs incrementally from its cursor and dbt incremental models use `_fivetran_synced` watermarks. That's why watermark-based, idempotent design matters. Check source log retention and freshness alerts.

---

## 12. Monitoring, Alerting & Observability

| Layer | What to monitor | How |
|---|---|---|
| **Fivetran** | Sync failures, delays, schema changes, MAR by table, re-syncs | Fivetran notifications/webhooks, **Fivetran Platform connector** (`fivetran_log` dbt package → dashboards) |
| **Snowflake (ingest)** | Credits on `FIVETRAN_WH`, long merges | `WAREHOUSE_METERING_HISTORY`, resource monitors, `QUERY_HISTORY` by user `FIVETRAN_USER` |
| **dbt** | Model failures, test failures (trend), run duration by model, freshness | `run_results.json` → Snowflake (dbt_artifacts / **Elementary**), `query_tag` per model, `store_failures` |
| **Airflow** | DAG/task failures, duration, deadline/SLA misses, queue backlog | Callbacks to Slack/PagerDuty, StatsD/OpenTelemetry metrics → Datadog/Grafana |
| **Data** | Volume anomalies, freshness, distribution drift | dbt tests, Elementary anomaly tests, Snowflake DMFs, Monte Carlo, etc. |

**Alert design (senior point):** route alerts to **owners** (dbt `meta: {owner: ...}`, Airflow `owner`, tags), severity-based (warn → channel, error on critical models → page), and with context (model, error, link to logs, last success time). Avoid alert fatigue: tune thresholds and dedupe repeated failures.

```yaml
# dbt model meta used by alerting
models:
  - name: fct_revenue
    config:
      meta: {owner: "finance-data@company.com", tier: "gold", sla_hours: 6}
      tags: ["finance_critical", "daily"]
```

---

## 13. Cost Optimization

**Fivetran**
- Sync only the tables and columns you need. Hash or block PII columns you don't need.
- Match sync frequency to the SLA (15-min syncs rarely justify their cost for daily dashboards).
- Watch MAR hotspots (chatty tables, history mode). Avoid unnecessary re-syncs.

**Snowflake (from Fivetran + dbt)**
- XS `FIVETRAN_WH` with 60s auto-suspend. Separate `TRANSFORM_WH` for dbt, with per-model warehouse overrides for heavy models only.
- Align schedules: run dbt right after syncs, so warehouses wake once rather than being kept warm by staggered jobs.
- Incremental models on large Fivetran tables, plus `incremental_predicates`. Staging as views.
- dbt tables are transient by default (no Fail-safe storage).

**Airflow**
- Deferrable operators and sensors (fewer workers needed).
- Don't run dbt every 15 minutes "just because." Use selective, tag-based runs.

**Typical quantifiable story:** "Moved Fivetran from 15-min to 1-hour syncs for 40 non-critical tables, deselected 120 unused columns, and converted 6 dbt table models to incremental. That cut MAR ~30% and Snowflake credits ~35% with no SLA impact."

---

## 14. Troubleshooting Scenarios

| # | Symptom | Likely cause | Fix |
|---|---|---|---|
| 1 | dbt mart shows deleted records | `_fivetran_deleted` not handled, or incremental model never deletes | Filter or propagate `is_deleted`. Add delete logic (post-hook/custom merge). Reconcile |
| 2 | Incremental model suddenly took 10× longer | Fivetran **re-sync** reset `_fivetran_synced` for all rows | Expected once. Use `incremental_predicates`. Plan a full refresh. Monitor re-sync events |
| 3 | Duplicates in a fact table | `append` strategy + re-sync, or a source without PK (`_fivetran_id` changes) | Switch to `merge` with a proper `unique_key`. Dedupe in staging |
| 4 | dbt ran on stale data even though DAG was green | Sync "succeeded" but the table was paused/excluded, or the connection was still on its own schedule | `dbt source freshness` gate. Set the Fivetran schedule to manual when Airflow triggers |
| 5 | dbt fails: "invalid identifier COLUMN_X" | Source column renamed or removed. Fivetran added a new column, the old one is stale | Update staging. Contracts and CI to catch it. Schema-change alerts |
| 6 | Airflow worker OOM / scheduler slow | Cosmos `DBT_LS` parsing a huge project at every DAG parse, or dbt running in the worker process | Use `DBT_MANIFEST` load mode. Containerized/virtualenv execution. Split DAGs |
| 7 | Snowflake queueing during the dbt run | Too many threads/parallel Cosmos tasks for warehouse size | Airflow **pool** for Snowflake. Tune dbt `threads`. Multi-cluster or bigger warehouse for heavy models |
| 8 | FivetranOperator times out | Long historical sync or source rate limits | Increase timeout, deferrable mode, separate initial loads from the daily DAG |
| 9 | Credits high on `FIVETRAN_WH` | High sync frequency on large tables keeps the warehouse awake | Reduce frequency, consolidate syncs, check merge efficiency |
| 10 | Timestamps shift by hours between source and mart | SaaS sources return UTC or local time inconsistently. `TIMESTAMP_LTZ` vs `NTZ` | Standardize to UTC `TIMESTAMP_NTZ` in staging (`convert_timezone`) |
| 11 | Dependency conflict installing dbt in Airflow | Shared Python environment | Separate virtualenv (`dbt_executable_path`) or Docker/K8s execution |
| 12 | Postgres source disk filling | Replication slot retaining WAL (connector failing or paused) | Fix the connector. Monitor slot lag. Set `max_slot_wal_keep_size` |

---

## 15. Senior Design Questions

**D1. Design the ELT platform for 30 SaaS sources and 5 production databases, with federated domain teams.**
> - **Ingestion**: Fivetran for SaaS + CDC DBs into `RAW` (schema per connection). Connections managed by Terraform. Platform team owns connection setup standards (schema-change policy, PII column blocking, sync frequency tiers).
> - **Transformation**: dbt. A platform-owned "core" project with staging of all sources and conformed dimensions, plus domain projects (dbt Mesh) building marts with public, contracted models. Fivetran packages for staging where useful, pinned.
> - **Orchestration**: Airflow (Astronomer/MWAA). DAG per domain and cadence. Cosmos task groups. Asset-based cross-domain dependencies. Pools per warehouse.
> - **Governance**: RBAC (Fivetran write-only to RAW, dbt read RAW / write ANALYTICS, domains own their schemas), tag-based PII masking, contracts and CI templates.
> - **Observability**: Fivetran log connector + dbt artifacts (Elementary) + Airflow metrics in one health dashboard. Owner-routed alerts.
> - **Cost**: sync tiers by SLA, per-domain warehouses, monthly MAR/credit review.

**D2. Leadership asks for "real-time" dashboards from the Postgres OLTP. Current setup: Fivetran every 6h → dbt daily. What do you propose?**
> Clarify what "real-time" means (often 15–60 minutes is enough). Options in increasing cost and complexity: (1) Fivetran 15-min sync + dbt hourly incremental for just those models (tag-based DAG). (2) Fivetran 5-min sync + Snowflake **dynamic tables** (dbt `dynamic_table` materialization) with `target_lag = '10 minutes'`. (3) Kafka/Debezium + Snowpipe Streaming for seconds-level latency. Recommend the cheapest option that meets the actual SLA, scoped to the few tables that matter, with cost estimates (MAR + credits).

**D3. Fivetran vs Airbyte vs custom (Snowpipe/Kafka/Openflow)?**
> Fivetran: most reliable managed connectors with low maintenance, but MAR cost grows with volume. Airbyte: open source or cloud, more control, cheaper at scale, more ops work. Custom/Snowpipe/Kafka: best for high-volume event streams or unsupported sources, but you own maintenance. Snowflake's native **Openflow** (NiFi-based ingestion) is a newer option worth evaluating. Typical answer: Fivetran for SaaS and moderate-volume databases, Snowpipe/Kafka for high-volume events, and custom only when necessary.

**D4. How do you onboard a new Fivetran source end to end (your team's runbook)?**
> 1. Intake: owner, business use, SLA, PII classification, required tables.
> 2. Create the connection via Terraform (schema name convention, sync frequency tier, schema change = allow columns, PII columns blocked or hashed).
> 3. Initial historical sync off-hours. Validate row counts with the source owner.
> 4. dbt: declare sources with freshness on `_fivetran_synced`, generate staging with `codegen`, add PK tests, handle `_fivetran_deleted`, apply PII tags and masking.
> 5. Airflow: add to the domain DAG (Fivetran operator → Cosmos group). Set the Fivetran schedule to manual.
> 6. Docs, owners in `meta`, alerts, add to the cost dashboard.
> 7. PR review and deploy through CI/CD.

---

## 16. Cheat Sheet

**Fivetran**
- One schema per connection in `RAW`. Fivetran merges by PK on **your** Snowflake warehouse (MAR cost + credits)
- `_fivetran_synced` → freshness + incremental watermark · `_fivetran_deleted` → soft deletes · `_fivetran_id` → no-PK tables · `_fivetran_start/_end/_active` → history mode SCD2
- Schema change modes: Allow all / Allow columns / Block all · type widening · removed columns stay (stop updating)
- Re-sync = all rows get new `_fivetran_synced` → incremental reprocesses everything, and `append` duplicates. Use `merge` + plan full refresh
- History mode captures **every** change. dbt snapshots only capture state at run time
- Fivetran Platform connector (`fivetran_log`) for monitoring and MAR. Terraform provider for connections as code
- Airflow-triggered syncs: set the Fivetran schedule to **manual**

**dbt on Fivetran data**
- Staging = views, rename/cast/UTC, handle `_fivetran_deleted`, carry `_fivetran_synced as _loaded_at`
- Incremental: `merge` + `unique_key` + lookback on `_loaded_at` + `incremental_predicates`
- Fivetran dbt packages: pin versions, configure `vars`, build your own marts on top
- Multiple connections of the same source → `dbt_utils.union_relations`

**Airflow**
- Orchestrate, don't compute · idempotent tasks · use the data interval (`{{ ds }}`), not `now()`
- Deferrable operators/sensors · pools for Snowflake · `max_active_runs=1` · retries with backoff · failure callbacks
- Assets (Airflow 3) / Datasets (2.x) for data-aware scheduling · dynamic task mapping `.expand()`
- Airflow 3: Assets, DAG versioning, new UI, Task SDK, `catchup` default False, `logical_date`

**dbt in Airflow**
- BashOperator (simple, use `dbt retry`) · **Cosmos** (task per model, `DBT_MANIFEST` load mode, `AFTER_EACH` tests, virtualenv/K8s execution) · dbt Cloud operator · KubernetesPodOperator
- Secrets via Airflow Connections → Cosmos profile mapping / `env_var()`
- Backfills: `--vars '{"run_date": "{{ ds }}"}'` or **microbatch** (`--event-time-start/end`). Separate warehouse for big backfills
- Flow: **Fivetran sync (wait) → source freshness gate → dbt build (per model + tests) → publish asset → alerts**
