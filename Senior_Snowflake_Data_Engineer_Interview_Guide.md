# Senior Data Engineer (Snowflake): Interview Preparation Guide

> **Target role:** Senior Data Engineer, Snowflake (5+ yrs). This is a single reference for Snowflake, data warehousing, dimensional modeling, performance tuning, ETL/ELT, dbt, Git, and CI/CD for data pipelines.
>
> **Senior-level focus:** Interviewers for senior roles care less about definitions and more about **design trade-offs, scale, governance, reliability, cost, and leading across teams** (the JD highlights *federated teams*). The 🔴 questions and sections 18–20 are where senior candidates stand apart.
>
> **How to use this guide**
> - Each section starts with **core concepts**, followed by **interview Q&A**. Answers are written the way you'd say them in an interview: short first, then depth.
> - 🟢 = basic (they'll expect it instantly) · 🟡 = intermediate (most questions land here) · 🔴 = advanced / senior-level differentiator
> - Practice saying answers **out loud**. For every concept, have one **"in my last project…"** example ready.
> - Snowflake ships features fast. The facts here are accurate as of mid-2026; verify anything edition-specific or newly released against docs.snowflake.com before the interview.

---

## Table of Contents

1. [Snowflake Architecture & Fundamentals](#1-snowflake-architecture--fundamentals)
2. [Virtual Warehouses & Compute](#2-virtual-warehouses--compute)
3. [Storage, Micro-partitions, Time Travel, Fail-safe, Cloning](#3-storage-micro-partitions-time-travel-fail-safe-cloning)
4. [Data Loading & Unloading (Stages, COPY, Snowpipe)](#4-data-loading--unloading)
5. [Streams, Tasks, Dynamic Tables (CDC & Orchestration)](#5-streams-tasks-dynamic-tables)
6. [Semi-structured Data](#6-semi-structured-data)
7. [Stored Procedures, UDFs, Snowpark](#7-stored-procedures-udfs-snowpark)
8. [Security, Governance, Data Sharing](#8-security-governance-data-sharing)
9. [Cost Management & Monitoring](#9-cost-management--monitoring)
10. [Performance Tuning (Snowflake)](#10-performance-tuning)
11. [Data Warehousing Concepts](#11-data-warehousing-concepts)
12. [Dimensional Modeling](#12-dimensional-modeling)
13. [ETL / ELT & Pipeline Design](#13-etl--elt--pipeline-design)
14. [dbt](#14-dbt)
15. [Git](#15-git)
16. [CI/CD for Data Pipelines](#16-cicd-for-data-pipelines)
17. [Data Quality, Governance & Metadata](#17-data-quality-governance--metadata)
18. [Scenario / System Design Questions](#18-scenario--system-design-questions)
19. [Troubleshooting Scenarios](#19-troubleshooting-scenarios)
20. [Behavioral Questions (mapped to the JD)](#20-behavioral-questions)
21. [Questions to Ask the Interviewer](#21-questions-to-ask-the-interviewer)
22. [Last-Night Cheat Sheet](#22-last-night-cheat-sheet)
23. [7-Day Study Plan](#23-7-day-study-plan)

---

## 1. Snowflake Architecture & Fundamentals

### Core concepts

Snowflake is a fully managed, cloud-native data platform (SaaS) running on AWS, Azure, or GCP. Its architecture is a **hybrid of shared-disk and shared-nothing**:

| Layer | What it does | Key points |
|---|---|---|
| **Database Storage** | Stores data in compressed, columnar **micro-partitions** in cloud object storage (S3 / Blob / GCS) | Fully managed; you never touch files directly; billed per TB/month (compressed) |
| **Compute (Virtual Warehouses)** | MPP clusters that run queries/DML/loads | Independent; scale up/down/out; billed per-second (60s minimum) in credits |
| **Cloud Services** | The "brain": authentication, metadata, query parsing/optimization, access control, transaction management, result cache | Billed only if it exceeds **10% of daily warehouse compute** |

**Shared-disk** → all compute sees one copy of data (central storage).
**Shared-nothing** → each warehouse is an MPP cluster with its own local cache/CPU.
**Result:** storage and compute scale **independently**, and many workloads run **without contention** (ELT warehouse ≠ BI warehouse).

**Editions** (features accumulate upward):
- **Standard**: core features, Time Travel up to 1 day
- **Enterprise**: multi-cluster warehouses, Time Travel up to 90 days, materialized views, search optimization, query acceleration, column-level security (masking), row access policies, ACCESS_HISTORY, periodic rekeying
- **Business Critical**: HIPAA/PCI support, Tri-Secret Secure (customer-managed keys), PrivateLink, database failover/failback
- **Virtual Private Snowflake (VPS)**: fully isolated environment

**Object hierarchy:**
`Organization → Account → Database → Schema → (Tables, Views, Stages, File Formats, Sequences, Pipes, Streams, Tasks, UDFs, Procedures, Policies, …)`
Warehouses, users, roles, integrations, resource monitors are **account-level** objects.

### Q&A

**🟢 Q1. Explain Snowflake's architecture.**
> Three decoupled layers. **Storage** keeps data as compressed columnar micro-partitions in cloud object storage. **Compute** is virtual warehouses, independent MPP clusters that process queries. **Cloud services** handles metadata, optimization, security, transactions, and caching. Because storage and compute are separate, I can give ELT and BI their own warehouses that read the same data without contention, and scale each independently.

**🟢 Q2. What makes Snowflake different from a traditional on-prem warehouse (Teradata/Oracle) or Redshift (classic)?**
> - Separation of storage and compute (scale independently; pay per second).
> - Near-zero administration: no indexes, vacuuming, distribution keys, or manual partitioning.
> - Instant elasticity: resize or add clusters in seconds.
> - Native semi-structured support (VARIANT).
> - Zero-copy cloning, Time Travel, secure data sharing without copying.
> - Multi-cloud and cross-region replication.

**🟢 Q3. What is the Cloud Services layer responsible for?**
> Authentication, access control, infrastructure management, metadata management (micro-partition stats like min/max per column), query parsing and optimization, transaction management, and the **result cache**. Metadata-only queries like `SELECT COUNT(*) FROM t` or `MIN/MAX` on a column can be answered here **without a running warehouse**.

**🟡 Q4. How is Cloud Services billed?**
> It consumes credits, but you're only charged for the portion that exceeds **10% of daily warehouse credit usage**. Heavy metadata operations (very frequent small queries, lots of `SHOW` commands, complex compilation, many COPY commands listing files) can push it over.

**🟡 Q5. Does Snowflake enforce primary keys / foreign keys / unique constraints?**
> No. Only **NOT NULL** is enforced (on standard tables; hybrid tables enforce PK/FK/unique). PK/FK/UNIQUE are informational: they help BI tools and documentation, and with `RELY` the optimizer can use them for join elimination. So **dedupe and integrity checks are the pipeline's responsibility** (e.g., dbt `unique` tests, MERGE logic).

**🟡 Q6. Types of tables in Snowflake?**

| Type | Time Travel | Fail-safe | Persistence | Use |
|---|---|---|---|---|
| **Permanent** | 0–1 day (Std), 0–90 (Ent+) | 7 days | Until dropped | Production data |
| **Transient** | 0–1 day | None | Until dropped | Staging / intermediate / re-creatable data (cheaper) |
| **Temporary** | 0–1 day | None | Session only | Session scratch work |
| **External** | No | No | Data in external storage | Query lake files in place |
| **Dynamic** | Yes | Yes | Declarative, auto-refreshed | Declarative pipelines |
| **Iceberg** | Snowflake-managed or external catalog | – | Open Apache Iceberg format in your storage | Lakehouse interoperability |
| **Hybrid** | – | – | Row-store + column-store, enforced PK | Low-latency transactional (Unistore) |

> A temporary table with the same name as a permanent table **shadows** it within the session.

**🟡 Q7. Types of views?**
> - **Standard view**: stored query, no data.
> - **Secure view**: hides the definition and prevents optimizer push-downs that could leak data. Required for data sharing. Can be slightly slower.
> - **Materialized view** (Enterprise): pre-computed, auto-maintained by a background service. Limited to a **single table** (no joins), no window functions, no UDFs, and so on. Good for expensive aggregations or for re-clustering a table differently.
> - **Secure materialized view**: both combined.

**🟢 Q8. What is a "stage"?**
> A location for data files to load or unload. **Internal**: user stage `@~`, table stage `@%table`, named internal stage `@my_stage`. **External**: named stage pointing to S3/Azure/GCS, ideally via a **storage integration** so no keys are embedded.

**🔴 Q9. How does Snowflake handle concurrency and transactions?**
> ACID-compliant with **READ COMMITTED** isolation for tables. Each statement sees data committed before it began. Micro-partitions are immutable, so writes create new partitions and update metadata (MVCC-like). That lets readers and writers not block each other. DML on the same table can queue on locks (`LOCK_TIMEOUT`, default 12 hours). Each DDL auto-commits. Explicit transactions use `BEGIN … COMMIT/ROLLBACK`.

**🔴 Q10. What is Snowflake's newer feature landscape you should be able to name?** (Know at a headline level.)
> - **Snowpark** (Python/Java/Scala DataFrames executed in Snowflake), **Snowpark Container Services**
> - **Dynamic Tables** (declarative pipelines)
> - **Iceberg Tables** plus **Snowflake Open Catalog** (Polaris-based)
> - **Cortex AI** (LLM functions such as `AI_COMPLETE`/`COMPLETE`, `SUMMARIZE`, `SENTIMENT`, Cortex Search, Cortex Analyst)
> - **Hybrid Tables** (Unistore)
> - **Native Git integration** (`CREATE GIT REPOSITORY`), **Snowflake CLI**, **dbt Projects on Snowflake**
> - **Horizon** (governance umbrella: tagging, classification, lineage, data quality monitoring with Data Metric Functions)
> - **Notebooks**, **Streamlit in Snowflake**

---

## 2. Virtual Warehouses & Compute

### Core concepts

| Size | Credits/hour (standard warehouse) |
|---|---|
| X-Small | 1 |
| Small | 2 |
| Medium | 4 |
| Large | 8 |
| X-Large | 16 |
| 2X-Large | 32 |
| 3X-Large | 64 |
| 4X-Large | 128 |
| 5X-Large | 256 |
| 6X-Large | 512 |

- Each size up **doubles** compute and credits. A query that's well parallelized often runs ~2× faster, so **cost stays similar while time halves**.
- **Billing:** per-second, **60-second minimum** each time a warehouse starts or resumes.
- **AUTO_SUSPEND** (seconds idle before suspend) and **AUTO_RESUME**.
- **Snowpark-optimized warehouses**: 16× memory per node, for ML and memory-heavy Snowpark.
- **Multi-cluster warehouse (Enterprise):** `MIN_CLUSTER_COUNT`, `MAX_CLUSTER_COUNT`, `SCALING_POLICY`
  - **Standard**: starts clusters immediately when queries queue (favors performance)
  - **Economy**: starts a cluster only if it would be busy for ≥6 minutes (favors cost)
  - **Maximized mode**: min = max (always N clusters). **Auto-scale mode**: min < max.

**Scale UP vs scale OUT:**

| | Scale UP (bigger size) | Scale OUT (more clusters) |
|---|---|---|
| Solves | Slow **complex** queries, spilling, big joins/sorts | **Concurrency**: many users/queries queuing |
| Mechanism | More CPU/memory/local SSD per query | More clusters behind one warehouse |
| Edition | All | Enterprise+ |

### Q&A

**🟢 Q1. What is a virtual warehouse?**
> A named cluster of compute resources (CPU, memory, local SSD cache) that executes queries, DML, and loads. Warehouses are independent of each other and of storage, and can be started, stopped, or resized any time without affecting data.

**🟢 Q2. Scale up vs scale out: when do you use each?**
> Scale **up** when individual queries are slow because of data volume or complexity, especially when the query profile shows **spilling to local/remote storage**. Scale **out** with multi-cluster when the problem is **queuing** (many concurrent users, BI dashboards at 9 AM). Bigger doesn't fix queueing, and more clusters don't make a single query faster.

**🟡 Q3. Does resizing a warehouse affect running queries?**
> No. Running queries finish on the current resources. New and queued queries use the new size. Resizing a suspended warehouse takes effect when it resumes.

**🟡 Q4. How would you set up warehouses for a typical data platform?**
> Separate by workload so each is isolated and cost is attributable:
> - `LOAD_WH` (XS–S, Snowpipe/COPY, short auto-suspend)
> - `TRANSFORM_WH` (M–L for dbt/ELT, auto-suspend 60s)
> - `BI_WH` (multi-cluster S–M, Standard scaling, for dashboards)
> - `DS_WH` / Snowpark-optimized for data science
> - `DEV_WH` (XS, aggressive auto-suspend)
>
> Each gets a **resource monitor**. Grant `USAGE` only to the relevant roles. Use `QUERY_TAG` for attribution.

**🟡 Q5. What should AUTO_SUSPEND be set to?**
> Trade-off between credit savings and losing the **warehouse cache**. For ETL/batch, 60 seconds is typical. For BI with frequent repeated queries, a few minutes (e.g., 5–10) keeps the local SSD cache warm. Avoid setting it below 60 seconds, since each resume bills a 60-second minimum anyway.

**🔴 Q6. What is the Query Acceleration Service (QAS)?**
> Enterprise feature that offloads parts of eligible queries (large scans with selective filters, and outlier queries) to **serverless compute**. This lets a warehouse handle occasional huge ad-hoc queries without upsizing. Controlled by `ENABLE_QUERY_ACCELERATION` and `QUERY_ACCELERATION_MAX_SCALE_FACTOR`. Use `SYSTEM$ESTIMATE_QUERY_ACCELERATION('<query_id>')` and the `QUERY_ACCELERATION_ELIGIBLE` view to check whether it helps.

**🔴 Q7. Warehouse parameters for governance?**
> - `STATEMENT_TIMEOUT_IN_SECONDS`: kill runaway queries (default 2 days. Always lower it).
> - `STATEMENT_QUEUED_TIMEOUT_IN_SECONDS`
> - `MAX_CONCURRENCY_LEVEL` (default 8): lowering it gives each query more resources. Raising it allows more concurrency per cluster.
> - `RESOURCE_MONITOR`

---

## 3. Storage, Micro-partitions, Time Travel, Fail-safe, Cloning

### Micro-partitions

- Contiguous storage units of **50–500 MB uncompressed** (much smaller compressed), **columnar**, **immutable**, created automatically in **insertion order**.
- Metadata stored per micro-partition: **min/max per column**, distinct count, null count, and more.
- **Partition pruning**: the optimizer uses min/max metadata to skip micro-partitions that can't match a filter. This is the #1 performance lever in Snowflake.
- Because they're immutable, `UPDATE`/`DELETE` rewrite whole micro-partitions. Frequent small updates are expensive and create churn (and Time Travel storage).

### Clustering

- **Natural clustering**: data loaded in date order is naturally clustered by date.
- **Clustering key**: `ALTER TABLE t CLUSTER BY (col1, col2)`. **Automatic Clustering** (serverless, billed) then reorganizes data in the background.
- **Metrics**: `SYSTEM$CLUSTERING_INFORMATION('t', '(col)')` returns `average_depth`, `average_overlaps`, and the partition depth histogram. **Lower depth = better clustering.** `SYSTEM$CLUSTERING_DEPTH`.
- **When to cluster:** multi-TB tables, queries consistently filtering or joining on the same columns, poor pruning, and a table that is **queried far more than it is updated**.
- **Choosing keys:** columns used most in WHERE/JOIN. Order **low → high cardinality**. Max 3–4 columns. Avoid very high-cardinality columns (e.g., a raw timestamp. Use `TO_DATE(ts)` instead) and avoid extremely low-cardinality ones (boolean).

### Time Travel

- Query, clone, or restore data as of a past point within the retention period.
  ```sql
  SELECT * FROM orders AT(TIMESTAMP => '2026-11-19 08:00:00'::timestamp_ltz);
  SELECT * FROM orders AT(OFFSET => -60*30);          -- 30 minutes ago
  SELECT * FROM orders BEFORE(STATEMENT => '<query_id>'); -- before a bad DML
  UNDROP TABLE orders;  UNDROP SCHEMA s;  UNDROP DATABASE d;
  CREATE TABLE orders_restore CLONE orders AT(OFFSET => -3600);
  ```
- `DATA_RETENTION_TIME_IN_DAYS`: Standard 0–1. Enterprise+ 0–90 for permanent objects. Transient/temporary 0–1.
- `MIN_DATA_RETENTION_TIME_IN_DAYS` (account-level) sets a floor.

### Fail-safe

- **7 days** after Time Travel ends, for **permanent** tables only. **Not user-accessible**: recovery only through Snowflake Support, best effort, for disaster recovery. Not configurable. Transient and temporary tables have **no** Fail-safe (cheaper).

**Data lifecycle:** `Current data → Time Travel (0–90 days, self-service) → Fail-safe (7 days, Snowflake support) → purged`

### Zero-copy cloning

- `CREATE TABLE|SCHEMA|DATABASE x CLONE y [AT|BEFORE …]`. A **metadata-only** copy that points to the same micro-partitions. Instant, and no extra storage until either side changes (then only changed partitions are stored).
- Use cases: dev/test environments from prod, CI test databases, point-in-time backups before risky releases, debugging.
- Cloned objects **do not inherit the source's grants** (unless `COPY GRANTS` for tables). Child objects within a cloned database/schema retain their privileges.
- When cloning a database/schema: **named internal stages are not cloned**. External stages are. Pipes referencing external stages are cloned (but **paused**). Tasks are cloned **suspended**. Streams are cloned, but unconsumed records in the clone may be inaccessible.

### Q&A

**🟢 Q1. What are micro-partitions and why do they matter?**
> Snowflake automatically divides tables into immutable, columnar micro-partitions of 50–500 MB uncompressed. It stores min/max and other stats for every column in each partition. During a query, the optimizer **prunes** partitions whose ranges can't match the filter, so a query over a 10 TB table might scan only a few GB. You don't define them. You influence them through load order and clustering keys.

**🟡 Q2. When would you define a clustering key? When wouldn't you?**
> Define one when the table is large (typically multi-TB), queries repeatedly filter on the same columns, the query profile shows poor pruning (partitions scanned ≈ partitions total), and the table is read much more than it is written. Don't define one on small tables, highly volatile tables (re-clustering cost churns), or when natural load order already gives good pruning. Validate with `SYSTEM$CLUSTERING_INFORMATION` before and after, and watch `AUTOMATIC_CLUSTERING_HISTORY` for cost.

**🟡 Q3. How do you check if a table is well clustered?**
> `SELECT SYSTEM$CLUSTERING_INFORMATION('db.sch.orders', '(order_date)');` Look at `average_depth` (close to 1 is ideal) and the `partition_depth_histogram`. Also check real queries: in the Query Profile compare **Partitions scanned vs Partitions total** on the TableScan node.

**🟡 Q4. Difference between Time Travel and Fail-safe?**
> Time Travel is **user-accessible** (query/clone/undrop), configurable from 0 to 90 days depending on edition and table type. Fail-safe is a fixed **7-day** period **after** Time Travel, for permanent tables only, recoverable only through Snowflake Support for disaster recovery. Both consume storage.

**🟡 Q5. Someone ran a bad `UPDATE` without a WHERE clause on a production table 2 hours ago. How do you recover?**
> 1. Find the query ID in `QUERY_HISTORY`.
> 2. `CREATE TABLE orders_fixed CLONE orders BEFORE(STATEMENT => '<query_id>');` (instant, zero-copy)
> 3. Validate the counts and data.
> 4. `ALTER TABLE orders SWAP WITH orders_fixed;` (atomic). Or, if only some rows were affected, `MERGE` the correct values back.
> 5. Check downstream impact (streams, dbt models) and do a root-cause fix (permissions, code review).

**🟡 Q6. How does zero-copy cloning work and how is it billed?**
> The clone is a metadata operation that references the same micro-partitions, so it's instant and initially costs no storage. When either the source or the clone is modified, only the new or changed micro-partitions consume storage. Great for spinning up full-size dev/QA copies or CI environments in seconds.

**🔴 Q7. Why can storage costs be high even though the table "isn't that big"?**
> Time Travel and Fail-safe retain old micro-partitions. A table that's **frequently updated or fully reloaded** (e.g., `CREATE OR REPLACE` or truncate/insert daily) keeps many copies. Check `TABLE_STORAGE_METRICS` for `ACTIVE_BYTES`, `TIME_TRAVEL_BYTES`, `FAILSAFE_BYTES`, `RETAINED_FOR_CLONE_BYTES`. Fixes: make staging tables **transient**, lower retention on high-churn tables, use incremental loads instead of full rebuilds.

**🔴 Q8. What is the Search Optimization Service?**
> Enterprise, serverless feature that builds a persistent search access path to speed up **highly selective point lookups** (`WHERE id = 123`), substring/regex searches, VARIANT field lookups, and geospatial predicates on large tables. Complements clustering (clustering helps range scans on a few columns). It has storage and compute costs. Enable per column: `ALTER TABLE t ADD SEARCH OPTIMIZATION ON EQUALITY(customer_id);`

**🔴 Q9. Materialized view vs clustering vs search optimization: how do you choose?**

| Need | Tool |
|---|---|
| Range filters on a few columns of a big table | Clustering key |
| Needle-in-haystack equality/substring lookups | Search optimization |
| Repeated expensive aggregation on one table, or the same table needed with a different clustering | Materialized view |
| Multi-table pre-computation with a freshness target | Dynamic table (or dbt incremental model) |

---

## 4. Data Loading & Unloading

### Core concepts

**Bulk load (COPY INTO)**: uses your warehouse. Best for batch.
```sql
CREATE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE STORAGE_PROVIDER = 'S3' ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://my-bucket/raw/');

CREATE FILE FORMAT ff_csv TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"' NULL_IF = ('', 'NULL') EMPTY_FIELD_AS_NULL = TRUE;

CREATE STAGE raw_stage URL = 's3://my-bucket/raw/'
  STORAGE_INTEGRATION = s3_int FILE_FORMAT = ff_csv;

COPY INTO raw.orders
FROM @raw_stage/orders/
PATTERN = '.*orders_.*[.]csv[.]gz'
ON_ERROR = 'CONTINUE';     -- or ABORT_STATEMENT (default for COPY), SKIP_FILE, SKIP_FILE_<n>, SKIP_FILE_<n>%
```

Key COPY options:
- `ON_ERROR`: `ABORT_STATEMENT` (COPY default), `CONTINUE`, `SKIP_FILE` (Snowpipe default), `SKIP_FILE_n`, `'SKIP_FILE_n%'`
- `VALIDATION_MODE = RETURN_ERRORS | RETURN_n_ROWS | RETURN_ALL_ERRORS`: dry run, loads nothing
- `PURGE = TRUE`: delete files after a successful load
- `FORCE = TRUE`: reload files even if already loaded (risk of duplicates)
- `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`: for Parquet/Avro/ORC/JSON, and CSV with header parsing
- `SIZE_LIMIT`, `TRUNCATECOLUMNS`, `ENFORCE_LENGTH`
- Transformations during load: `COPY INTO t FROM (SELECT $1, $2, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP() FROM @stage)`
- `INCLUDE_METADATA` / `METADATA$FILE_LAST_MODIFIED`, `METADATA$START_SCAN_TIME`

**Load metadata (deduplication):** Snowflake tracks which files were loaded into each table. **COPY: 64 days** of load metadata. **Snowpipe: 14 days.** Files already loaded are skipped automatically (unless `FORCE`). If a file's load status is unknown (older than 64 days and the file's LAST_MODIFIED date is older than 64 days), use `LOAD_UNCERTAIN_FILES = TRUE`.

**Snowpipe**: serverless, continuous micro-batch loading.
```sql
CREATE PIPE raw.orders_pipe AUTO_INGEST = TRUE AS
COPY INTO raw.orders FROM @raw_stage/orders/ FILE_FORMAT = ff_csv;
-- SHOW PIPES → notification_channel (SQS ARN) → configure S3 event notification
```
- Triggered by **cloud event notifications** (S3 → SQS, Azure Event Grid, GCS Pub/Sub) or the **REST API** (`insertFiles`).
- Billed on serverless compute plus a per-file overhead charge (simplified to a per-GB model for many accounts. Check the current pricing doc).
- Monitor with `SYSTEM$PIPE_STATUS('pipe')`, `COPY_HISTORY`, `PIPE_USAGE_HISTORY`. Recover missed files with `ALTER PIPE … REFRESH` (files staged in the last 7 days).
- Latency is typically about a minute.

**Snowpipe Streaming**: row-level streaming via the Java SDK / Kafka connector (in Snowpipe Streaming mode), no files, seconds latency, lowest cost for streaming. **Kafka Connector** can use either Snowpipe (files) or Snowpipe Streaming.

**File sizing best practice:** **~100–250 MB compressed** per file for parallelism. Avoid huge single files (can't parallelize) and millions of tiny files (overhead). Split large files.

**Unloading:**
```sql
COPY INTO @export_stage/orders/ FROM (SELECT * FROM orders WHERE order_date = CURRENT_DATE - 1)
FILE_FORMAT = (TYPE = PARQUET) HEADER = TRUE MAX_FILE_SIZE = 268435456 OVERWRITE = TRUE;
-- PARTITION BY ('date=' || TO_VARCHAR(order_date)) for partitioned output
```
Then use `GET` (internal stage → local). `PUT` uploads local → internal stage (SnowSQL/CLI only, not in the web UI worksheet).

### Q&A

**🟢 Q1. What are the ways to load data into Snowflake?**
> 1. **Bulk** `COPY INTO` from internal or external stages (batch)
> 2. **Snowpipe** auto-ingest (continuous micro-batch, serverless)
> 3. **Snowpipe Streaming** / Kafka connector (row-level, low latency)
> 4. Third-party ELT tools (Fivetran, Airbyte, Matillion, Informatica, ADF)
> 5. External tables / Iceberg tables to query in place
> 6. Web UI load for small files, or `INSERT` for tiny volumes

**🟡 Q2. COPY INTO vs Snowpipe?**

| | COPY INTO | Snowpipe |
|---|---|---|
| Compute | Your warehouse | Serverless |
| Trigger | Scheduled/manual | Event notification or REST |
| Latency | Batch (minutes–hours) | ~1 minute |
| Load metadata | 64 days | 14 days |
| Default ON_ERROR | ABORT_STATEMENT | SKIP_FILE |
| Transactions | One per COPY statement | Per file / micro-batch, order not guaranteed |

**🟡 Q3. How does Snowflake prevent loading the same file twice?**
> It keeps load metadata per table (file name plus ETag/checksum). COPY skips files already loaded in the last 64 days (Snowpipe 14 days). `FORCE = TRUE` overrides it. If a file is renamed or modified it will load again, so upstream idempotency still matters. I also add `METADATA$FILENAME` to the table for lineage.

**🟡 Q4. A COPY command fails partway. What happens and how do you debug it?**
> With `ABORT_STATEMENT`, the whole statement is rolled back. Debug with `VALIDATION_MODE = RETURN_ERRORS` or `SELECT * FROM TABLE(VALIDATE(raw.orders, JOB_ID => '_last'))`, and check `COPY_HISTORY` for the first error, line, and column. Common causes: delimiter inside unquoted fields, wrong date format, column count mismatch, encoding, or string too long. Fixes: adjust the file format, use `ON_ERROR = CONTINUE` into a raw VARIANT/string landing table, and route bad rows to a quarantine table.

**🟡 Q5. Snowpipe isn't loading files. How do you troubleshoot?**
> 1. `SELECT SYSTEM$PIPE_STATUS('raw.orders_pipe');` Is it `RUNNING` or paused? Check `pendingFileCount` and `lastIngestedTimestamp`.
> 2. Check the cloud event notification is configured to the pipe's `notification_channel` and the prefix/suffix filters match.
> 3. Check `COPY_HISTORY` / `PIPE_USAGE_HISTORY` for errors (file format, permissions).
> 4. Check the storage integration's IAM trust policy (external ID / role).
> 5. Check whether the pipe owner role still has privileges (ownership transfer pauses pipes).
> 6. Backfill with `ALTER PIPE raw.orders_pipe REFRESH;` (last 7 days of files).

**🔴 Q6. How would you load JSON with schema evolution?**
> Land raw JSON into a `VARIANT` column (plus filename and load timestamp), then flatten in transformations. That way new fields never break ingestion. Alternatively, for Parquet/Avro/ORC/CSV/JSON use `INFER_SCHEMA` + `CREATE TABLE … USING TEMPLATE` and enable `ENABLE_SCHEMA_EVOLUTION = TRUE` on the table with `MATCH_BY_COLUMN_NAME`, so COPY adds new columns automatically.

**🔴 Q7. External table vs Iceberg table vs loading into native table?**
> - **Native table**: best performance, full features. Data lives in Snowflake storage.
> - **External table**: read-only over files in your lake, slower (no Snowflake micro-partition optimization). Needs metadata refresh. Good for occasional queries over lake data.
> - **Iceberg table**: data stays in your storage in open Iceberg format, with near-native performance. Snowflake-managed catalog supports full DML, or you can use an external catalog (Glue, Open Catalog). Choose it when other engines (Spark, Trino) must read and write the same data.

---

## 5. Streams, Tasks, Dynamic Tables

### Streams (CDC)

A **stream** records DML changes (inserts, updates, deletes) on a table/view since the stream's **offset**. It stores no data. It's an offset plus a view over the table's versioning.

```sql
CREATE STREAM raw.orders_stream ON TABLE raw.orders;              -- standard (delta)
CREATE STREAM raw.orders_ins ON TABLE raw.orders APPEND_ONLY = TRUE; -- inserts only (cheaper)
CREATE STREAM ext_s ON EXTERNAL TABLE ext_t INSERT_ONLY = TRUE;   -- external/Iceberg tables
```
Metadata columns: `METADATA$ACTION` (INSERT/DELETE), `METADATA$ISUPDATE` (TRUE if part of an update), `METADATA$ROW_ID`.
An **UPDATE** appears as a DELETE + INSERT pair with `METADATA$ISUPDATE = TRUE`.

- The offset **advances only when the stream is consumed in a DML statement that commits** (e.g., `INSERT INTO … SELECT FROM stream`, `MERGE … USING stream`). A plain `SELECT` does **not** advance it.
- **Staleness:** a stream becomes stale if not consumed within the table's data retention period. Snowflake can extend retention up to `MAX_DATA_EXTENSION_TIME_IN_DAYS` (default 14) for unconsumed streams. Check `STALE_AFTER` in `SHOW STREAMS`. A stale stream must be recreated.
- Streams can be on tables, views (including secure views), directory tables, external tables, dynamic tables, and Iceberg tables.
- Multiple streams on one table = independent consumers.

### Tasks (scheduling/orchestration)

```sql
CREATE TASK transform.load_orders
  WAREHOUSE = TRANSFORM_WH            -- omit for serverless tasks (USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE)
  SCHEDULE = 'USING CRON 0 * * * * UTC'   -- or '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('raw.orders_stream')   -- skip run (no compute) if no changes
AS
  MERGE INTO core.orders t
  USING (SELECT * FROM raw.orders_stream WHERE METADATA$ACTION = 'INSERT') s
  ON t.order_id = s.order_id
  WHEN MATCHED THEN UPDATE SET t.status = s.status, t.updated_at = s.updated_at
  WHEN NOT MATCHED THEN INSERT (order_id, status, updated_at) VALUES (s.order_id, s.status, s.updated_at);

CREATE TASK transform.build_agg WAREHOUSE = TRANSFORM_WH AFTER transform.load_orders AS CALL build_agg();
ALTER TASK transform.build_agg RESUME;   -- tasks are created SUSPENDED; resume children before root
ALTER TASK transform.load_orders RESUME;
EXECUTE TASK transform.load_orders;     -- manual run
```
- **Task graphs (DAGs):** one root task with a schedule, children via `AFTER` (multiple predecessors allowed). Supports **finalizer tasks** (`FINALIZE = root_task`) for cleanup/notification.
- **Serverless tasks**: Snowflake manages compute size. Good for short, frequent tasks. Can set `TARGET_COMPLETION_INTERVAL`.
- **Triggered tasks**: run when a stream has data, with no schedule.
- Monitoring: `TASK_HISTORY()` (Information Schema), `ACCOUNT_USAGE.TASK_HISTORY`, `COMPLETE_TASK_GRAPHS`. Error notifications through `ERROR_INTEGRATION`. `SUSPEND_TASK_AFTER_NUM_FAILURES`.
- Use `SYSTEM$TASK_RUNTIME_INFO`, task return values, and `CONFIG` for parameterization.

### Dynamic Tables

```sql
CREATE DYNAMIC TABLE core.customer_orders
  TARGET_LAG = '15 minutes'        -- or DOWNSTREAM
  WAREHOUSE = TRANSFORM_WH
  REFRESH_MODE = AUTO              -- INCREMENTAL | FULL
AS
SELECT c.customer_id, c.name, COUNT(o.order_id) AS order_cnt, SUM(o.amount) AS total
FROM raw.customers c JOIN raw.orders o ON c.customer_id = o.customer_id
GROUP BY 1, 2;
```
- **Declarative**: you define the result. Snowflake handles the refresh (incremental when possible) to stay within `TARGET_LAG`.
- `TARGET_LAG = DOWNSTREAM` refreshes only when downstream dynamic tables need it.
- Monitor via `DYNAMIC_TABLE_REFRESH_HISTORY`, `DYNAMIC_TABLE_GRAPH_HISTORY`, and Snowsight's graph view.
- Some constructs force **full** refresh (certain non-deterministic functions, some unsupported operators). Check `refresh_mode_reason`.

**Streams + Tasks vs Dynamic Tables vs dbt:**

| | Streams + Tasks | Dynamic Tables | dbt |
|---|---|---|---|
| Style | Imperative (you write MERGE logic) | Declarative SQL | Declarative SQL + Jinja + testing + docs |
| Control | Maximum (custom SCD, deletes, error handling) | Less control, simpler | Full SDLC (Git, tests, CI) |
| Scheduling | Cron/interval/triggered | Target lag | External orchestrator or dbt Cloud/Snowflake |
| Best for | Complex CDC, procedural logic | Continuous transforms with freshness SLAs | Batch modeling, team collaboration |

> dbt can also materialize **dynamic tables** (`materialized='dynamic_table'`), giving you both.

### Q&A

**🟢 Q1. What is a stream?**
> A change-tracking object that records inserts, updates, and deletes on a source since its last consumption offset. It adds `METADATA$ACTION`, `METADATA$ISUPDATE`, and `METADATA$ROW_ID`. It doesn't copy data. The offset moves forward only when the stream is used in a committed DML statement.

**🟡 Q2. Standard vs append-only stream?**
> Standard tracks all DML and returns the **net** change (an insert followed by a delete in the same window returns nothing). Append-only tracks inserts only. It's cheaper and ideal for insert-only landing tables (logs, events, Snowpipe targets). Insert-only is the equivalent for external and Iceberg tables.

**🟡 Q3. How do you build an incremental CDC pipeline natively in Snowflake?**
> Snowpipe lands files into a raw table → a stream on the raw table → a task (with `WHEN SYSTEM$STREAM_HAS_DATA`) runs a `MERGE` from the stream into the curated table, handling inserts, updates, and deletes → child tasks build downstream aggregates. Add error integration for alerts, an audit table for row counts, and keep logic idempotent. Alternatively, define the curated layer as dynamic tables with a target lag.

**🟡 Q4. What happens if two tasks consume the same stream?**
> The first committed DML advances the offset, so the second sees nothing (or partial). Fix: create **one stream per consumer**, or consume the stream once into a staging table inside a transaction and fan out from there.

**🟡 Q5. How do you consume a stream into multiple targets atomically?**
> Wrap them in an explicit transaction: `BEGIN; INSERT INTO t1 SELECT … FROM s; INSERT INTO t2 SELECT … FROM s; COMMIT;`. Within a transaction the stream returns the same change set, and the offset advances at commit. Or use a multi-table `INSERT ALL`.

**🔴 Q6. How do you handle deletes from a stream in a MERGE?**
```sql
MERGE INTO core.customers t
USING (
  SELECT * FROM raw.customers_stream
  WHERE NOT (METADATA$ACTION = 'DELETE' AND METADATA$ISUPDATE)   -- drop the "delete half" of updates
) s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.METADATA$ACTION = 'DELETE' THEN DELETE        -- or soft delete: UPDATE SET is_deleted = TRUE
WHEN MATCHED AND s.METADATA$ACTION = 'INSERT' THEN UPDATE SET t.name = s.name, t.email = s.email
WHEN NOT MATCHED AND s.METADATA$ACTION = 'INSERT' THEN
  INSERT (customer_id, name, email) VALUES (s.customer_id, s.name, s.email);
```

**🔴 Q7. A stream became stale. Why, and what do you do?**
> It wasn't consumed within the source table's retention (plus the extension up to `MAX_DATA_EXTENSION_TIME_IN_DAYS`). The change history is no longer available. Recreate the stream, then **reconcile** by doing a full compare or reload between source and target for the gap period. Prevention: monitor `STALE_AFTER`, alert on task failures, consume regularly.

**🔴 Q8. Task graph failed in the middle. How do you rerun?**
> Check `TASK_HISTORY` / `COMPLETE_TASK_GRAPHS` for the failed task and error. Fix it, then use `EXECUTE TASK` on the root, or `ALTER TASK … RETRY LAST` style reruns (`EXECUTE TASK <root> RETRY LAST` reruns from failed tasks). Make tasks idempotent (MERGE, not blind INSERT) so a rerun doesn't duplicate data.

---

## 6. Semi-structured Data

### Core concepts

- Data types: **VARIANT** (any value), **OBJECT** (key-value), **ARRAY**. A single VARIANT value historically had a 16 MB limit. Snowflake has been raising maximum sizes for VARIANT/VARCHAR (up to 128 MB) via behavior changes, so confirm the current limit in docs.
- Snowflake stores VARIANT in columnar form internally (sub-columns are extracted for common paths), so querying fields is fast and pruning works on them.
- Formats: JSON, Avro, ORC, Parquet, XML.

```sql
-- Dot / bracket notation, then cast
SELECT v:customer.name::STRING AS name,
       v:customer.address.city::STRING AS city,
       v['order_id']::NUMBER AS order_id
FROM raw.events;

-- FLATTEN arrays
SELECT e.v:order_id::NUMBER AS order_id,
       li.value:sku::STRING AS sku,
       li.value:qty::INT AS qty,
       li.index AS line_no
FROM raw.events e,
     LATERAL FLATTEN(input => e.v:line_items) li;

-- Nested flatten
SELECT f1.value:id, f2.value:tag
FROM raw.t, LATERAL FLATTEN(input => v:items) f1, LATERAL FLATTEN(input => f1.value:tags) f2;

-- Keep rows with empty arrays
LATERAL FLATTEN(input => v:items, OUTER => TRUE)

-- Build JSON
SELECT OBJECT_CONSTRUCT('id', id, 'name', name) AS j, ARRAY_AGG(sku) AS skus FROM … GROUP BY …;

-- Useful functions
PARSE_JSON, TRY_PARSE_JSON, TO_VARIANT, TYPEOF, IS_NULL_VALUE, OBJECT_KEYS, ARRAY_SIZE, GET_PATH, STRIP_NULL_VALUE
```
**FLATTEN output columns:** `SEQ, KEY, PATH, INDEX, VALUE, THIS`.

### Q&A

**🟢 Q1. How do you store and query JSON in Snowflake?**
> Load into a VARIANT column (often with metadata like filename and load timestamp). Query with path notation `v:field.subfield::TYPE`, and use `LATERAL FLATTEN` to explode arrays into rows. In the curated layer I extract frequently used fields into typed columns for performance and clarity.

**🟡 Q2. JSON null vs SQL NULL?**
> A JSON `null` is stored as a VARIANT containing the string "null" (not a SQL NULL). `v:field IS NULL` is false for a JSON null. Use `IS_NULL_VALUE(v:field)`, or cast (`v:field::STRING` returns SQL NULL). `STRIP_NULL_VALUE` converts it. The file-format option `STRIP_NULL_VALUES = TRUE` removes them on load.

**🟡 Q3. Should you keep data in VARIANT or flatten into columns?**
> Land as VARIANT for resilience to schema changes. Flatten into typed columns in silver/gold for: frequently filtered/joined fields, dates and timestamps (pruning and type safety), and BI usability. Keep the raw VARIANT for reprocessing.

**🔴 Q4. Performance concerns with VARIANT?**
> Snowflake sub-columnarizes common paths, but if documents are highly heterogeneous, mix types in the same path, or have very many distinct keys, fewer paths get extracted, and pruning and performance degrade. Dates stored as strings in JSON won't prune well until cast. Remedies: extract hot fields into real columns, use search optimization on VARIANT paths, and keep consistent types.

---

## 7. Stored Procedures, UDFs, Snowpark

### Core concepts

| | UDF | UDTF | Stored Procedure |
|---|---|---|---|
| Returns | Scalar value | Table (rows) | Single value / table |
| Called with | In SQL `SELECT my_udf(x)` | `FROM TABLE(my_udtf(x))` | `CALL my_proc()` |
| Can run DDL/DML | No | No | Yes |
| Languages | SQL, JavaScript, Python, Java, Scala | Same | **Snowflake Scripting (SQL)**, JavaScript, Python, Java, Scala |
| Use | Reusable calculation | Parsing/explode logic | Procedural workflows, dynamic SQL, admin automation |

- **Caller's rights vs owner's rights** (`EXECUTE AS CALLER | OWNER`, default OWNER). Owner's rights procedures run with the owner role's privileges (good for controlled elevated actions, with restrictions such as no session variable access). Caller's rights run with the caller's privileges and session context.
- **Secure UDFs/procedures** hide their definition.
- **External functions** call remote APIs (AWS API Gateway / Azure). Now often replaced by **external access integrations** (network rules + secrets) inside Python UDFs/procs.
- **Vectorized Python UDFs** (batch via pandas) for performance.

**Reusable, modular stored procedure example (the JD asks for this explicitly):**
```sql
CREATE OR REPLACE PROCEDURE util.sp_merge_incremental(
    p_source STRING, p_target STRING, p_key STRING, p_watermark_col STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  v_wm      TIMESTAMP_NTZ;
  v_cols    STRING;
  v_set     STRING;
  v_sql     STRING;
  v_rows    INTEGER DEFAULT 0;
BEGIN
  -- 1. Read high watermark from control table
  SELECT COALESCE(MAX(last_watermark), '1900-01-01'::TIMESTAMP_NTZ) INTO :v_wm
  FROM util.etl_control WHERE target_table = :p_target;

  -- 2. Build column lists dynamically from INFORMATION_SCHEMA
  SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position),
         LISTAGG('t.' || column_name || ' = s.' || column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position)
    INTO :v_cols, :v_set
  FROM information_schema.columns
  WHERE table_schema || '.' || table_name = UPPER(:p_target);

  -- 3. Dynamic MERGE
  v_sql := 'MERGE INTO ' || p_target || ' t USING (SELECT * FROM ' || p_source ||
           ' WHERE ' || p_watermark_col || ' > ''' || v_wm || '''' ||
           ' QUALIFY ROW_NUMBER() OVER (PARTITION BY ' || p_key || ' ORDER BY ' || p_watermark_col || ' DESC) = 1) s' ||
           ' ON t.' || p_key || ' = s.' || p_key ||
           ' WHEN MATCHED THEN UPDATE SET ' || v_set ||
           ' WHEN NOT MATCHED THEN INSERT (' || v_cols || ') VALUES (s.' || REPLACE(v_cols, ', ', ', s.') || ')';
  EXECUTE IMMEDIATE :v_sql;
  v_rows := SQLROWCOUNT;

  -- 4. Update watermark & audit
  MERGE INTO util.etl_control c
  USING (SELECT :p_target AS target_table) x ON c.target_table = x.target_table
  WHEN MATCHED THEN UPDATE SET last_watermark = CURRENT_TIMESTAMP(), last_run = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (target_table, last_watermark, last_run)
                         VALUES (:p_target, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

  INSERT INTO util.etl_audit(target_table, rows_affected, status, run_ts)
  VALUES (:p_target, :v_rows, 'SUCCESS', CURRENT_TIMESTAMP());
  RETURN 'Merged ' || v_rows || ' rows into ' || p_target;

EXCEPTION
  WHEN OTHER THEN
    INSERT INTO util.etl_audit(target_table, rows_affected, status, error_msg, run_ts)
    VALUES (:p_target, 0, 'FAILED', :SQLERRM, CURRENT_TIMESTAMP());
    RAISE;
END;
$$;
```
> Talking points: parameterized (one proc serves many tables), metadata-driven columns, idempotent MERGE with dedupe, watermark control table, audit logging, exception handling that **re-raises** so the orchestrator sees failure. In production, prefer the source's max watermark over `CURRENT_TIMESTAMP()` to avoid missing late rows, and sanitize identifiers (`IDENTIFIER()` / validate inputs) to avoid SQL injection.

**Snowflake Scripting essentials:** `DECLARE`, `BEGIN…END`, `LET`, `IF/ELSEIF`, `CASE`, `FOR rec IN cursor DO`, `WHILE`, `REPEAT`, `LOOP`, `RESULTSET`, `EXECUTE IMMEDIATE`, `SQLROWCOUNT`, `SQLFOUND`, `SQLCODE/SQLERRM/SQLSTATE`, custom `EXCEPTION`, `RETURN TABLE(resultset)`.

**Snowpark:** DataFrame API in Python/Java/Scala that **pushes down** to Snowflake SQL (lazy evaluation, executed on `collect()`/`save_as_table()`), so data never leaves Snowflake. Used for complex transformations, ML feature engineering, and Python logic without moving data to Spark.

### Q&A

**🟢 Q1. Difference between a UDF and a stored procedure?**
> A UDF returns a value, is used inside SQL expressions, and can't run DDL/DML. A stored procedure is called with `CALL`, can contain procedural logic, transactions, dynamic SQL, DDL and DML, and is used for workflows and administrative automation.

**🟡 Q2. Owner's rights vs caller's rights?**
> Owner's rights (default) runs with the owning role's privileges. Useful to let users perform a controlled action (e.g., refresh a table) without granting them direct privileges. Restrictions: can't access the caller's session variables or change session state. Caller's rights runs with the caller's privileges and session. Use it for generic utilities that should respect the user's access.

**🟡 Q3. How do you make stored procedures reusable and modular?**
> Parameterize table names, keys, and dates. Drive logic from metadata (`INFORMATION_SCHEMA`, control tables). Keep a single responsibility per proc and compose them via a parent proc or task graph. Use standard logging/audit and error-handling patterns. Keep the code in Git and deploy via CI/CD. Write them idempotent. Validate identifiers against injection.

**🔴 Q4. When would you use Snowpark over SQL?**
> When logic is hard in SQL: complex Python libraries, ML (training/inference), API calls via external access, reusable Python packages, or teams who prefer a DataFrame API. For standard set-based transformations, SQL/dbt is simpler and equally fast because Snowpark compiles to SQL anyway.

---

## 8. Security, Governance, Data Sharing

### Access control model

- **RBAC + DAC**: privileges are granted to **roles**, roles are granted to **users** and to other roles (hierarchy). Every object has an **owner role** (DAC).
- **System-defined roles:**
  - `ORGADMIN`: organization-level (accounts, org usage)
  - `ACCOUNTADMIN`: top role (encapsulates SYSADMIN + SECURITYADMIN). Limit to a few users with MFA. Don't use it for daily work.
  - `SECURITYADMIN`: manage grants globally (`MANAGE GRANTS`), inherits USERADMIN
  - `USERADMIN`: create users and roles
  - `SYSADMIN`: create warehouses, databases, objects. **Custom roles should roll up to SYSADMIN.**
  - `PUBLIC`: granted to everyone
- **Database roles**: roles scoped to a database (portable with shares and clones).
- **Secondary roles**: `USE SECONDARY ROLES ALL` combines privileges from all granted roles.
- **Future grants**: `GRANT SELECT ON FUTURE TABLES IN SCHEMA s TO ROLE r;` (schema-level future grants take precedence over database-level).
- **Managed access schemas**: only the schema owner (or MANAGE GRANTS) can grant privileges on objects. This centralizes control.

**Recommended pattern: functional vs access roles:**
```
Access roles (privileges on objects):   DB_SALES_RO, DB_SALES_RW, DB_SALES_ADMIN
Functional roles (job functions):       ANALYST, DATA_ENGINEER, DBT_TRANSFORMER, BI_SERVICE
Functional roles are granted access roles → functional roles roll up to SYSADMIN
```

### Data protection features

| Feature | What it does | Edition |
|---|---|---|
| **Dynamic data masking** | Masking policy on a column. Returns masked value based on role/context at query time | Enterprise |
| **External tokenization** | Masking policy calls an external function to detokenize | Enterprise |
| **Row access policy** | Filters rows based on context (e.g., role → region mapping table) | Enterprise |
| **Tag-based masking** | Attach a masking policy to a **tag**. Every column with that tag is masked | Enterprise |
| **Object tagging** | Key-value tags for classification, cost attribution | Enterprise |
| **Data classification** | Auto-detects PII semantic categories (`SYSTEM$CLASSIFY` / automatic classification) | Enterprise |
| **Projection / aggregation policies** | Prevent selecting a column / require aggregation (privacy) | Enterprise |
| **Secure views/UDFs** | Hide definitions, prevent data leakage via optimizer | All |
| **Access History** | Who accessed which columns, lineage of writes | Enterprise |
| **Network policies / rules** | IP allow/block lists, private connectivity | All (PrivateLink: Business Critical) |
| **Encryption** | AES-256 at rest & TLS in transit, hierarchical key model, automatic key rotation (30 days); **periodic rekeying** yearly (Enterprise); **Tri-Secret Secure** (Business Critical) | All / Ent / BC |

```sql
CREATE MASKING POLICY gov.mask_email AS (val STRING) RETURNS STRING ->
  CASE WHEN IS_ROLE_IN_SESSION('PII_READER') THEN val
       ELSE REGEXP_REPLACE(val, '.+@', '*****@') END;
ALTER TABLE core.customers MODIFY COLUMN email SET MASKING POLICY gov.mask_email;

CREATE ROW ACCESS POLICY gov.rap_region AS (region STRING) RETURNS BOOLEAN ->
  IS_ROLE_IN_SESSION('GLOBAL_ANALYST')
  OR EXISTS (SELECT 1 FROM gov.region_map m
             WHERE m.role_name = CURRENT_ROLE() AND m.region = region);
ALTER TABLE core.sales ADD ROW ACCESS POLICY gov.rap_region ON (region);
```
> Use `IS_ROLE_IN_SESSION()` rather than `CURRENT_ROLE()` in policies so role hierarchy and secondary roles are respected.

**Authentication:** SSO via SAML (Okta/Azure AD), **MFA** (Snowflake is phasing out single-factor password logins for human users. Service users should use key-pair or OAuth/workload identity), **key-pair auth** for service accounts (dbt, Airflow, CI), OAuth, SCIM for user provisioning, `TYPE = SERVICE` users. **Programmatic access tokens** also exist for service use.

### Data sharing

- **Secure Data Sharing**: provider shares objects (tables, secure views, secure UDFs) with consumer accounts. **No data is copied.** The consumer queries live data with their own compute.
- **Reader accounts**: for consumers without a Snowflake account. The provider pays for compute.
- **Listings / Snowflake Marketplace**: private or public listings, with cross-region/cloud auto-fulfillment.
- **Cross-region/cloud** sharing requires **replication** (handled automatically by listings).
- **Data Clean Rooms**: privacy-preserving joint analysis.

**Replication & failover:** database / account object replication through **replication groups** and **failover groups** (Business Critical for failover). **Client redirect** via connection URLs for BCDR.

### Q&A

**🟢 Q1. Explain Snowflake's access control model.**
> Privileges are granted to roles, roles are granted to users and to other roles, forming a hierarchy. Every object is owned by a role. System roles are ORGADMIN, ACCOUNTADMIN, SECURITYADMIN, USERADMIN, SYSADMIN, PUBLIC. Best practice is custom access roles holding object privileges, granted to functional roles, all rolling up to SYSADMIN, with ACCOUNTADMIN tightly restricted.

**🟡 Q2. How do you implement column-level security for PII?**
> Tag PII columns (e.g., `PII = 'EMAIL'`) using classification to find them. Create masking policies that use `IS_ROLE_IN_SESSION('PII_READER')`, and attach them to the **tag** so new columns get masked automatically. Verify by querying as different roles. Audit with `ACCESS_HISTORY` and `POLICY_REFERENCES`.

**🟡 Q3. How do you implement row-level security?**
> Row access policy referencing a mapping table (role or user → allowed region/business unit), attached to the table's filter column. It's evaluated at query time, transparently to users. Keep the mapping table governed, and test performance since the policy acts like a join.

**🟡 Q4. What's a secure view and when is it required?**
> A view whose definition is hidden from non-owners and for which the optimizer won't apply rewrites that could expose underlying data (e.g., pushing a UDF that leaks rows). It's required to share views via Secure Data Sharing. Slight performance cost, so don't use it for purely internal views.

**🟡 Q5. How does data sharing work without copying data?**
> The provider creates a share and grants usage on a database/schema plus select on objects. The consumer creates a read-only database from the share. Metadata points to the provider's storage. The consumer pays only for their own compute (except reader accounts). Updates are visible instantly.

**🟡 Q6. How do you secure service accounts for dbt / Airflow / CI?**
> Create a `TYPE = SERVICE` user with **key-pair authentication** (or OAuth / workload identity federation), store the private key in a secrets manager (AWS Secrets Manager, Vault, GitHub secrets), give it a least-privilege functional role and a default warehouse, apply a network policy restricting IPs, and rotate keys (Snowflake supports two public keys, `RSA_PUBLIC_KEY` and `RSA_PUBLIC_KEY_2`, for zero-downtime rotation).

**🔴 Q7. Future grants pitfall?**
> If both database-level and schema-level future grants exist for the same object type, **schema-level wins** and database-level ones are ignored for that schema. Future grants don't apply to existing objects. Run a one-time `GRANT … ON ALL TABLES IN SCHEMA` too. With dbt using `CREATE OR REPLACE`, grants can be lost unless `copy_grants` or dbt's `grants` config is used.

**🔴 Q8. How would you design RBAC for a federated/data-mesh organization?** (Relevant: the JD says "Federated teams".)
> - Each domain (Sales, Finance, Marketing) gets its own database(s) and domain-admin, RW, and RO access roles (or **database roles** for portability).
> - The central platform team owns account-level objects: warehouses, integrations, network policies, masking policy library and tags (in a central `GOVERNANCE` database).
> - Domains publish **data products** as secure views in a `SHARED`/`PUBLISHED` schema. Consumers get RO roles or internal listings.
> - Use managed access schemas so domain admins control grants within their area. Centrally enforced tag-based masking so PII is protected regardless of domain.
> - Provision everything as code (Terraform / schemachange), and attribute cost by domain with warehouses and tags.

---

## 9. Cost Management & Monitoring

### Core concepts

**Cost components:** compute (warehouses), serverless (Snowpipe, automatic clustering, MV maintenance, search optimization, serverless tasks, QAS, dynamic tables on serverless where applicable, replication), cloud services (over 10%), storage (active + Time Travel + Fail-safe), data transfer (egress cross-region/cloud).

**Key views (`SNOWFLAKE.ACCOUNT_USAGE`, latency ~45 min–3 hrs, 365-day retention):**
`QUERY_HISTORY`, `WAREHOUSE_METERING_HISTORY`, `WAREHOUSE_LOAD_HISTORY`, `METERING_DAILY_HISTORY`, `STORAGE_USAGE`, `TABLE_STORAGE_METRICS`, `DATABASE_STORAGE_USAGE_HISTORY`, `PIPE_USAGE_HISTORY`, `AUTOMATIC_CLUSTERING_HISTORY`, `MATERIALIZED_VIEW_REFRESH_HISTORY`, `SERVERLESS_TASK_HISTORY`, `LOGIN_HISTORY`, `ACCESS_HISTORY`, `COPY_HISTORY`, `QUERY_ATTRIBUTION_HISTORY` (per-query compute cost).
**`INFORMATION_SCHEMA`**: real-time, but shorter retention (7 days to 6 months depending on function), and scoped to a database.
**`ORGANIZATION_USAGE`**: across accounts.

**Resource monitors:**
```sql
CREATE RESOURCE MONITOR rm_transform WITH CREDIT_QUOTA = 500 FREQUENCY = MONTHLY START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 75 PERCENT DO NOTIFY ON 90 PERCENT DO SUSPEND ON 100 PERCENT DO SUSPEND_IMMEDIATE;
ALTER WAREHOUSE TRANSFORM_WH SET RESOURCE_MONITOR = rm_transform;
```
> Resource monitors control **warehouses only**, not serverless features. Use **budgets** for serverless and broader spend alerts.

### Q&A

**🟡 Q1. How do you reduce Snowflake costs?**
> 1. **Right-size warehouses**: start small, scale up only when the profile shows spilling. Check `WAREHOUSE_LOAD_HISTORY` for low utilization.
> 2. **Auto-suspend** at 60 seconds for ETL warehouses. Consolidate idle warehouses.
> 3. **Fix bad queries**: top-N by credits/time from `QUERY_HISTORY`/`QUERY_ATTRIBUTION_HISTORY`. Improve pruning, avoid `SELECT *`, fix exploding joins.
> 4. **Incremental processing** instead of full rebuilds (dbt incremental, streams).
> 5. **Storage**: transient tables for staging, lower Time Travel on high-churn tables, drop unused tables and clones.
> 6. **Review serverless**: clustering keys that churn, unnecessary MVs/search optimization.
> 7. **Governance**: resource monitors, budgets, statement timeouts, query tags for chargeback.
> 8. Leverage the **result cache** for BI. Schedule jobs to share running warehouses.

**🟡 Q2. Query to find the most expensive queries?**
```sql
SELECT query_id, user_name, warehouse_name, warehouse_size,
       total_elapsed_time/1000 AS secs, bytes_scanned/POWER(1024,3) AS gb_scanned,
       partitions_scanned, partitions_total,
       bytes_spilled_to_local_storage, bytes_spilled_to_remote_storage, query_text
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND execution_status = 'SUCCESS'
ORDER BY total_elapsed_time DESC
LIMIT 50;

-- Credits per query (attribution)
SELECT query_id, warehouse_name, credits_attributed_compute
FROM snowflake.account_usage.query_attribution_history
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY credits_attributed_compute DESC LIMIT 50;

-- Credits by warehouse
SELECT warehouse_name, SUM(credits_used) AS credits
FROM snowflake.account_usage.warehouse_metering_history
WHERE start_time >= DATEADD(month, -1, CURRENT_TIMESTAMP())
GROUP BY 1 ORDER BY 2 DESC;
```

**🟡 Q3. ACCOUNT_USAGE vs INFORMATION_SCHEMA?**
> ACCOUNT_USAGE: account-wide, includes dropped objects, 1-year retention, but latency of up to roughly 45 minutes to 3 hours. INFORMATION_SCHEMA: per database, real-time, no dropped objects, shorter retention (e.g., `QUERY_HISTORY()` table function covers 7 days).

**🔴 Q4. How would you implement cost chargeback across federated teams?**
> Dedicated warehouses per domain (simplest attribution), `QUERY_TAG` set by dbt/Airflow (`{"team":"finance","model":"fct_orders"}`), object tags on warehouses and databases (`cost_center`), then build a cost dashboard from `WAREHOUSE_METERING_HISTORY` + `QUERY_ATTRIBUTION_HISTORY` + `TAG_REFERENCES`. Serverless and storage are attributed by database/tag.

---

## 10. Performance Tuning

### The mental model

Snowflake performance = **scan less** (pruning, projection) + **compute efficiently** (joins, aggregation, no spill) + **reuse** (caches, pre-computation) + **right resources** (warehouse size, concurrency).

### The three caches

| Cache | Where | Lifetime | Notes |
|---|---|---|---|
| **Result cache** | Cloud services | **24 hours**, reset each reuse, up to **31 days** max | Same query text (exact), same role privileges, underlying data unchanged, no non-deterministic functions (e.g., `CURRENT_TIMESTAMP()`, `RANDOM()`), no external functions. **No warehouse needed.** Disable: `USE_CACHED_RESULT = FALSE` (do this when benchmarking) |
| **Warehouse (local disk) cache** | Warehouse SSD/memory | Until the warehouse suspends (or resizes) | Caches raw micro-partition data. Argument for not suspending BI warehouses too aggressively |
| **Metadata cache** | Cloud services | Always | `COUNT(*)`, `MIN`/`MAX`, `SHOW`, `DESCRIBE` answered without compute |

### Reading the Query Profile (what to look for)

| Symptom in profile | Meaning | Fix |
|---|---|---|
| **Partitions scanned ≈ Partitions total** on TableScan | Poor pruning | Filter on clustered/naturally ordered column. Add clustering key. Avoid functions on the filter column. Search optimization for point lookups |
| **Bytes spilled to local storage** | Memory overflow to SSD | Bigger warehouse, reduce data earlier (filter/aggregate before join), fewer columns |
| **Bytes spilled to remote storage** | Severe. Spill to object storage | Definitely scale up and/or rewrite. Break the query into steps |
| **Join output rows ≫ input rows** ("exploding join") | Many-to-many / missing join key / duplicates | Fix join conditions, dedupe before joining |
| **Cartesian join** node | Missing join predicate | Fix the query |
| **Large Sort / ORDER BY / window** | Expensive global ops | Remove unnecessary ORDER BY, limit window partitions |
| **"Remote disk I/O" or "Network communication" dominating** | Data movement | Scan fewer columns, prune more |
| **Processing dominates, single node busy** | Data skew | Handle skewed keys (salting / split hot keys), check `NULL`-heavy join keys |
| **High compilation time** | Very complex SQL / huge IN lists / many views | Simplify, materialize intermediates |
| **Queued time (in QUERY_HISTORY)** | Concurrency | Multi-cluster, separate warehouses, tune `MAX_CONCURRENCY_LEVEL` |

### Query-level best practices

1. **Select only needed columns** (columnar storage, so `SELECT *` reads everything).
2. **Filter early, on pruning-friendly columns.** Avoid wrapping the filter column in a function: `WHERE DATE(created_at) = '2026-11-01'` may prune worse than `WHERE created_at >= '2026-11-01' AND created_at < '2026-11-02'`. Keep data types consistent (no implicit casts on the column side).
3. **Pre-aggregate before joining**, and join on equal data types.
4. `UNION ALL` instead of `UNION` when duplicates aren't possible (avoids a dedupe sort).
5. Use **`QUALIFY`** for window-function filtering instead of nested subqueries.
6. Replace `NOT IN` (null-unsafe and often slower) with `NOT EXISTS` or an anti-join.
7. Avoid `ORDER BY` in intermediate CTEs/views. Use `LIMIT` with `ORDER BY` only when needed.
8. Use `APPROX_COUNT_DISTINCT` (HyperLogLog) when exactness isn't required.
9. **Materialize** heavy reused CTEs (temp/transient table, dbt table) instead of recomputing.
10. For large DML: prefer `INSERT OVERWRITE` or `CREATE TABLE AS` over massive row-by-row `UPDATE`s. Batch MERGE with deduplicated sources. Avoid singleton inserts in loops.
11. Use `LIMIT`/`SAMPLE` during dev.
12. Large `IN` lists → put the values in a table and join.

### Table/warehouse-level levers

- Clustering keys, search optimization, materialized views, dynamic tables
- Load data in a sorted order (e.g., `ORDER BY event_date` on initial load) for natural clustering
- Right-size warehouse. Separate workloads. Multi-cluster for concurrency. QAS for outliers
- Transient tables for intermediates (cheaper storage, not faster)

### Q&A

**🟢 Q1. A query is slow. Walk me through how you'd tune it.**
> 1. **Reproduce and baseline**: run with `USE_CACHED_RESULT = FALSE`, note elapsed time, bytes scanned, warehouse size.
> 2. **Open the Query Profile**: find the most expensive operator nodes.
> 3. **Check pruning**: partitions scanned vs total. If poor, fix filters, add a clustering key, or use search optimization.
> 4. **Check spilling**: if spilling, reduce data volume earlier or scale up.
> 5. **Check joins**: exploding rows or Cartesian joins mean fixing keys and deduplicating.
> 6. **Check queue time**: if it's queueing, not execution, add multi-cluster or separate warehouses.
> 7. **Rewrite**: fewer columns, push filters down, pre-aggregate, `QUALIFY`, `UNION ALL`.
> 8. **Consider pre-computation**: incremental model, MV, dynamic table.
> 9. **Measure again** and document.

**🟡 Q2. What is spilling and how do you fix it?**
> When an operation (join, sort, aggregation) doesn't fit in warehouse memory, Snowflake spills to local SSD, and if that's not enough, to remote storage. It's much slower. Fix by processing less data (filter earlier, select fewer columns, pre-aggregate, split into steps) or by scaling the warehouse up (more memory). Remote spilling almost always justifies a size increase.

**🟡 Q3. What is partition pruning and what breaks it?**
> The optimizer skips micro-partitions whose min/max for the filter column can't match. It breaks when: filtering on columns with random distribution across partitions; wrapping the column in functions or casts the optimizer can't reason about; filters coming from a subquery or join that isn't known at compile time (although Snowflake does some runtime/dynamic pruning for joins); or filtering on non-deterministic expressions.

**🟡 Q4. Explain the result cache. When is it NOT used?**
> Query results are cached for 24 hours (extended on reuse up to 31 days). The exact same query (syntactically identical) by a role with the same access, on unchanged data, returns instantly with no warehouse. It's not used if the underlying data changed, the query has non-deterministic functions (`CURRENT_TIMESTAMP`, `RANDOM`, `UUID_STRING`), uses external functions, the query text differs even slightly, the role lacks privileges, or `USE_CACHED_RESULT = FALSE`.

**🟡 Q5. A BI dashboard is slow every morning at 9 AM but fine in the afternoon. Why?**
> Most likely **concurrency/queueing** (everyone opens dashboards at once) plus a **cold warehouse cache** after an overnight suspend. Check `QUEUED_OVERLOAD_TIME` in `QUERY_HISTORY` and `WAREHOUSE_LOAD_HISTORY`. Fix: multi-cluster warehouse (Standard policy), separate BI warehouse from ELT, pre-aggregate dashboard tables, and make sure ELT finishes before 9 AM. Optionally run a cache-warming query.

**🟡 Q6. How do you handle data skew in Snowflake?**
> Identify via the Query Profile (one worker processing far more rows) and by checking key distributions (`SELECT key, COUNT(*) … ORDER BY 2 DESC`). Common culprits are NULL or default keys (e.g., `-1`, `'UNKNOWN'`). Fixes: filter or handle NULL keys separately, split hot keys and process them separately, then `UNION ALL`, or pre-aggregate before the join.

**🟡 Q7. `MERGE` on a big table is slow. How do you speed it up?**
> - Deduplicate the source (multiple source rows per key also causes nondeterministic errors).
> - Restrict the target scan: add a predicate on a clustered column in the `ON` clause (e.g., `AND t.event_date >= DATEADD(day, -3, CURRENT_DATE)`) so pruning applies.
> - Cluster the target on the merge key's date dimension.
> - Process only the delta (stream or watermark).
> - If most rows change, `INSERT OVERWRITE` or a CTAS + `SWAP` can be faster than MERGE.
> - Size the warehouse up for big batch merges.

**🔴 Q8. Materialized views: limitations and costs?**
> Single source table only (no joins), no window functions, no `HAVING`, no non-deterministic functions, limited aggregates, and so on. Maintenance runs serverless and is billed. High-churn base tables make MVs expensive. Benefits: always current (Snowflake combines MV data with base-table changes not yet applied), automatic query rewrite can use them. For joins, use dynamic tables or dbt models instead.

**🔴 Q9. How do you benchmark a performance improvement fairly?**
> Disable the result cache (`ALTER SESSION SET USE_CACHED_RESULT = FALSE`), use the same warehouse size, run a couple of times to account for the warehouse cache (or suspend/resume to clear it, and be explicit about warm vs cold), compare elapsed time, bytes scanned, partitions scanned, spill, and credits, and test with production-like data volumes.

**🔴 Q10. What's join elimination / `RELY`?**
> If you declare PK/FK constraints with `RELY`, the optimizer can eliminate unnecessary joins (e.g., a view joins a dimension, but the query doesn't select dimension columns). It's only safe if the constraints are actually true, since Snowflake doesn't enforce them.

---

## 11. Data Warehousing Concepts

### Core concepts

**OLTP vs OLAP**

| | OLTP | OLAP / Data Warehouse |
|---|---|---|
| Purpose | Run the business (transactions) | Analyze the business |
| Workload | Many small reads/writes | Large scans, aggregations |
| Model | Normalized (3NF) | Denormalized (star), or Data Vault + marts |
| Storage | Row-oriented | Column-oriented |
| History | Current state | Full history |
| Example | Postgres, Oracle app DB | Snowflake, BigQuery, Redshift |

**Architectural approaches**

| Approach | Idea | Pros | Cons |
|---|---|---|---|
| **Inmon (top-down)** | Enterprise 3NF EDW first, then dependent data marts | Single integrated source of truth, flexible | Slow to deliver, heavy upfront modeling |
| **Kimball (bottom-up)** | Business-process dimensional marts integrated via **conformed dimensions** (bus architecture) | Fast value, business-friendly, performant | Harder to adapt when sources change a lot |
| **Data Vault 2.0** | Hubs (business keys), Links (relationships), Satellites (descriptive attributes + history). Insert-only, hash keys | Auditable, agile with many/changing sources, parallel loading | More tables/joins. Needs a presentation (Kimball) layer on top |
| **Medallion (Bronze/Silver/Gold)** | Raw → cleaned/conformed → business-ready | Simple mental model, works with ELT | Naming only. You still need modeling discipline inside each layer |
| **Data Mesh** | Domain-owned data products, self-serve platform, federated governance | Scales organizationally | Needs strong platform + governance or it fragments |

**Typical Snowflake layering (what I'd say I've built):**
```
RAW / BRONZE   → exact copy of sources (VARIANT/landing tables), append-only, transient where re-creatable
STAGING        → typed, renamed, deduplicated, light cleansing (1:1 with source tables)  [dbt stg_ models, views]
INTEGRATION    → conformed entities, business keys, (optional Data Vault)            [dbt int_ models]
MARTS / GOLD   → facts and dimensions per business domain, aggregates for BI         [dbt fct_/dim_ models, tables/incremental]
SEMANTIC/SHARE → secure views, metrics, data products for consumers
```

**Other must-know terms:** data lake, lakehouse (open table formats like Iceberg/Delta), ODS (operational data store), data mart, EDW, metadata, lineage, CDC, SCD, surrogate key, natural/business key, grain, conformed dimension, schema-on-read vs schema-on-write.

### Q&A

**🟢 Q1. What is a data warehouse?**
> A central, integrated, subject-oriented, time-variant, non-volatile store of data optimized for analytics (Inmon's definition). In practice it consolidates data from many sources, keeps history, models it for analysis, and serves BI, reporting, and data science.

**🟢 Q2. ETL vs ELT? Why ELT in Snowflake?**
> ETL transforms before loading (on an external engine). ELT loads raw data first and transforms inside the warehouse using its compute. Snowflake suits ELT because compute is elastic and separate, raw data is retained for reprocessing, transformations are SQL (dbt) that's versioned and testable, and you avoid maintaining a separate transformation cluster. ETL still makes sense for PII scrubbing before landing, or heavy non-SQL processing.

**🟡 Q3. Kimball vs Inmon vs Data Vault: which would you choose?**
> It depends on the drivers. For a business-facing analytics platform with relatively stable sources, I'd model **Kimball star schemas** in the marts. With many volatile sources, strong audit needs, or frequent source changes, I'd add a **Data Vault** integration layer underneath and build Kimball marts on top. Inmon-style 3NF EDWs are less common now in cloud ELT. In Snowflake the common pragmatic pattern is raw → staging → (vault or integration) → Kimball marts.

**🟡 Q4. What is a data lakehouse and where does Snowflake fit?**
> A lakehouse combines a lake's open, cheap storage with warehouse features (ACID, schema, performance) via open table formats like Iceberg. Snowflake supports this with **Iceberg tables** (Snowflake-managed or external catalog), so other engines like Spark can read and write the same tables while Snowflake provides governance and performance.

**🟡 Q5. Schema-on-read vs schema-on-write?**
> Schema-on-write enforces structure at load time (typed tables). Schema-on-read stores raw data and applies structure at query time (VARIANT, external tables). I land data schema-on-read for resilience, then enforce schema-on-write in curated layers.

**🔴 Q6. How do you design a warehouse for a federated (data mesh) organization?**
> - **Platform team** (likely this role): provides the self-serve platform, including account setup, RBAC templates, warehouses, ingestion frameworks, dbt project template, CI/CD, monitoring, governance policies.
> - **Domain teams** own their data products end to end: their dbt projects/models, tests, documentation, SLAs.
> - **Federated governance**: global standards (naming, PII tagging, masking, quality thresholds, contract requirements) enforced centrally through tag-based policies and CI checks. Domains decide locally within those rules.
> - **Interoperability**: conformed dimensions (customer, product, date) owned by one domain and shared. Data contracts on published models. dbt Mesh (cross-project `ref`) or internal listings for discovery.
> - **Observability**: freshness, quality, cost per domain.

**🔴 Q7. How do you handle late-arriving data?**
> - **Late facts** (event arrives days later): incremental models reprocess a **lookback window** (e.g., last 3 days by event date) rather than strict "> max(loaded_at)". Partition/cluster on event date so the reprocessing is cheap. Use `loaded_at` watermark for detection and `event_date` for placement.
> - **Late dimensions** (fact arrives before its dimension row): insert an **inferred/placeholder dimension member** (business key with "Unknown" attributes) so the fact gets a surrogate key, then update the placeholder when the real row arrives (SCD1 overwrite of the inferred row). Alternatively map to a `-1 Unknown` member and re-key later.

**🔴 Q8. What does idempotency mean for pipelines and how do you guarantee it?**
> Running the same job twice for the same input yields the same result, with no duplicates and no drift. Techniques: MERGE on business keys instead of INSERT; delete+insert for a partition/window; dedupe with `QUALIFY ROW_NUMBER()`; deterministic surrogate keys (hashes of business keys); load metadata (COPY's file tracking); watermarks stored only after success; transactions around multi-step writes. This lets you safely retry and backfill.

---

## 12. Dimensional Modeling

### Core concepts

**The Kimball four-step process:**
1. **Select the business process** (e.g., order fulfillment, claims, web sessions)
2. **Declare the grain**: what exactly one fact row represents (most important decision)
3. **Identify the dimensions**: who, what, where, when, why, how
4. **Identify the facts**: numeric measures consistent with the grain

**Star vs Snowflake schema**

| | Star | Snowflake (schema) |
|---|---|---|
| Dimensions | Denormalized (one table per dim) | Normalized into sub-dimensions |
| Joins | Fewer | More |
| Query simplicity/BI | Better | Worse |
| Storage | More redundancy (negligible in columnar stores) | Less |
| Recommendation in Snowflake | **Default** | Only when a sub-dimension is large/shared or changes independently |

> Snowflake the product is **not** tied to the snowflake schema. Star schemas are recommended.

**Fact table types**

| Type | Grain | Example | Behavior |
|---|---|---|---|
| **Transaction** | One row per event | Order line, payment | Insert-only, most granular |
| **Periodic snapshot** | One row per entity per period | Daily account balance, monthly inventory | Regular inserts, dense |
| **Accumulating snapshot** | One row per process instance, with milestone dates | Order lifecycle: ordered → shipped → delivered | Rows **updated** as milestones happen. Lag measures |
| **Factless fact** | Event/coverage with no measures | Student attendance, promotion coverage | Count rows. Track what didn't happen |
| **Aggregate fact** | Rolled-up | Daily sales by store | Performance |

**Measure additivity**
- **Additive**: can be summed across all dimensions (sales amount, quantity)
- **Semi-additive**: summable across some dimensions but not time (account balance, inventory level → use average or end-of-period over time)
- **Non-additive**: never summed (ratios, percentages, unit price → store the components, compute the ratio)

**Dimension types / techniques**
- **Conformed dimension**: same meaning and keys across facts/marts (`dim_date`, `dim_customer`). Enables drill-across.
- **Role-playing dimension**: one physical dim used in multiple roles (`dim_date` as order_date, ship_date, delivery_date) via views/aliases.
- **Junk dimension**: combines low-cardinality flags/indicators into one dim to avoid many tiny dims or wide facts.
- **Degenerate dimension**: dimension key stored in the fact with no dim table (order number, invoice number).
- **Mini-dimension**: splits rapidly changing attributes (age band, income band) out of a large dim to avoid SCD2 explosion.
- **Outrigger**: dimension referencing another dimension (use sparingly).
- **Bridge table**: resolves many-to-many (patient ↔ diagnoses, account ↔ customers) or ragged hierarchies. Includes weighting factors to avoid double counting.
- **Shrunken dimension**: subset of a dim's rows/columns for aggregate facts (`dim_month` from `dim_date`).
- **Static/reference dimension**, **Audit dimension** (load batch metadata).
- **Unknown member** rows (`-1 = Unknown`, `-2 = Not applicable`) so facts never have NULL foreign keys.

**Surrogate keys**
- Meaningless keys owned by the warehouse. Isolate from source key changes, enable SCD2 versions, merge multiple sources, and support unknown members.
- In Snowflake: **sequence/IDENTITY** (not guaranteed gap-free/ordered across sessions) or a **hash of business key(s)** (`MD5`/`SHA2` via `dbt_utils.generate_surrogate_key`). Hashes are deterministic (idempotent, parallel loads, no lookups) but wider. Common in modern ELT and Data Vault.

### Slowly Changing Dimensions (know all of these cold)

| Type | Behavior | History? | Example |
|---|---|---|---|
| **0** | Retain original, never change | No | Original credit score, date of birth |
| **1** | Overwrite | No | Fixing a typo in name |
| **2** | New row per change with `valid_from`, `valid_to`, `is_current` (and new surrogate key) | Full | Customer address (sales must attribute to the address at the time) |
| **3** | Add "previous value" column | Limited (one prior) | Current and previous sales region |
| **4** | Current table + separate history table (or mini-dimension) | Full (in separate table) | Fast-changing attributes |
| **6** (1+2+3) | SCD2 rows plus a "current value" column overwritten on all rows | Full + current view | Report by historical or current region |

**SCD Type 2 in Snowflake (MERGE pattern, conceptual):**
```sql
-- 1) Expire changed current rows
MERGE INTO dim_customer d
USING stg_customer s
  ON d.customer_id = s.customer_id AND d.is_current
WHEN MATCHED AND d.hash_diff <> s.hash_diff THEN
  UPDATE SET d.valid_to = s.updated_at, d.is_current = FALSE;

-- 2) Insert new versions (new + changed customers)
INSERT INTO dim_customer (customer_sk, customer_id, name, city, hash_diff, valid_from, valid_to, is_current)
SELECT MD5(s.customer_id || '|' || s.updated_at), s.customer_id, s.name, s.city, s.hash_diff,
       s.updated_at, '9999-12-31'::timestamp, TRUE
FROM stg_customer s
LEFT JOIN dim_customer d ON d.customer_id = s.customer_id AND d.is_current
WHERE d.customer_id IS NULL;       -- no current row → new or just expired
```
> Wrap both steps in one transaction. `hash_diff` = hash of tracked attributes to detect changes cheaply. In dbt, use **snapshots** for this.

**Fact-to-SCD2 lookup:** join the fact's business key + event timestamp to the dimension where `event_ts >= valid_from AND event_ts < valid_to` to get the surrogate key **as of the event**.

### Q&A

**🟢 Q1. Fact vs dimension?**
> Facts are the numeric measurements of a business process at a declared grain (amount, quantity, duration), plus foreign keys to dimensions. Dimensions are the descriptive context (who, what, where, when) used to filter, group, and label facts. Facts are long and narrow. Dimensions are wide and relatively short.

**🟢 Q2. What is grain and why is it the most important decision?**
> Grain defines what one fact row represents, e.g., "one row per order line per day." Every dimension and measure must be consistent with it. Mixing grains causes double counting and wrong aggregates. I always declare it in writing (and test it with a uniqueness test on the grain columns).

**🟢 Q3. Star vs snowflake schema?**
> Star: denormalized dimensions joined directly to the fact. Simpler and faster for BI. Snowflake: normalized dimensions into sub-tables. Saves little storage in a columnar engine and adds joins. In Snowflake I default to star schemas.

**🟡 Q4. Explain SCD types 1, 2, 3 with examples and when you'd use each.**
> Type 1 overwrites (corrections, attributes where history doesn't matter). Type 2 adds a new row with effective dates and a current flag (attributes that affect historical reporting, like customer segment or address). Type 3 keeps a previous-value column (limited "before/after" analysis, like a sales-territory realignment). Type 6 combines them when users need both as-was and as-is reporting.

**🟡 Q5. How do you model a many-to-many relationship?**
> Use a **bridge table** between the fact and the dimension (e.g., `bridge_account_customer` with account_key, customer_key, allocation_weight). For a fact like balance, weight the measure to avoid double counting, or report "impact" totals knowingly. Alternatively, change the grain of the fact so it's one-to-one.

**🟡 Q6. What are conformed dimensions and why do they matter in a federated setup?**
> Dimensions with consistent keys, attributes, and meaning across multiple fact tables and marts, e.g., one `dim_customer` used by Sales, Support, and Finance. They enable drill-across analysis and a consistent "customer count." In a federated model, one domain **owns** each conformed dimension and publishes it as a contracted data product. Others reference it instead of building their own.

**🟡 Q7. How do you handle a semi-additive measure like account balance?**
> Store it in a **periodic snapshot** fact (daily balance per account). Sum across accounts is fine. Across time, use the period-end value or the average daily balance, never the sum. Document it in the semantic layer so BI tools aggregate correctly.

**🟡 Q8. Accumulating snapshot: give an example.**
> Order fulfillment: one row per order with foreign keys to `order_date`, `pick_date`, `ship_date`, `delivery_date` (role-playing date dim), and lag measures (days to ship, days to deliver). The row is updated as each milestone happens. Great for pipeline and bottleneck analysis.

**🔴 Q9. Natural keys vs surrogate keys vs hash keys: what do you use in Snowflake and why?**
> I use **surrogate keys** in dimensions, generated as **hashes of business keys** (plus the effective date for SCD2 versions). They're deterministic, so loads are idempotent, parallel, and need no lookup against a sequence. They also work well across environments and with dbt (`generate_surrogate_key`). Sequences are fine too, but they need lookups and aren't reproducible across rebuilds. Trade-off: hash keys are wider strings and have a tiny collision risk (negligible with MD5 at typical scale, or use SHA2 for very large ones).

**🔴 Q10. A dimension has 50M rows and some attributes change daily. SCD2 is exploding. What do you do?**
> Split the rapidly changing attributes into a **mini-dimension** (banded values like age band or score band) referenced directly from the fact, or move them to a separate SCD4 history table. Keep only slowly changing, analytically relevant attributes in SCD2. Ask whether history is actually needed for each attribute (Type 1 where it isn't). Use `hash_diff` on only the tracked columns so irrelevant changes don't create versions.

**🔴 Q11. How do you model for both BI tools and data science in Snowflake?**
> Kimball marts for BI (stable, conformed, documented, governed). Wide, denormalized **One Big Table** or feature tables derived from the marts for data science and some dashboards (Snowflake handles wide tables well). Keep the star schema as the source of truth so metrics stay consistent. Add a semantic layer (dbt Semantic Layer, Snowflake semantic views) for metric definitions.

**🔴 Q12. How would you model a dimension with a ragged/variable-depth hierarchy (e.g., org chart)?**
> Options: a **bridge/closure table** (ancestor, descendant, depth) that allows rollups at any level; **flattened levels** (level1…levelN) if the depth is bounded; or recursive CTEs (`WITH RECURSIVE` or `CONNECT BY` in Snowflake) to build the closure table during transformation. Closure tables are the most flexible for BI.

---

## 13. ETL / ELT & Pipeline Design

### Core concepts

**Pipeline design principles (senior-level talking points)**
1. **Idempotent and re-runnable**: MERGE / delete-insert, deterministic keys
2. **Incremental by default**: watermarks, streams, CDC. Full refresh only as needed
3. **Separation of layers**: raw is immutable. Transformations are reproducible from raw
4. **Schema evolution handling**: land semi-structured, contracts at publishing boundaries
5. **Observability**: logs, row counts, freshness, quality checks, alerting, lineage
6. **Backfill-friendly**: parameterized date ranges, partition-level reprocessing
7. **Fail loudly, recover gracefully**: retries with backoff, dead-letter/quarantine tables, clear runbooks
8. **Security by design**: PII tagging and masking, least privilege, secrets management
9. **Cost-aware**: right-sized warehouses, avoid reprocessing, transient staging
10. **Everything as code**: Git, CI/CD, IaC, reviews

**Incremental load strategies**

| Strategy | How | Caveat |
|---|---|---|
| **High-watermark** | `WHERE updated_at > last_max` | Needs reliable update timestamp. Doesn't see hard deletes. Use a lookback for late rows |
| **CDC (log-based)** | Debezium/Fivetran/HVR/Streams capture inserts/updates/deletes | Best fidelity. Handle ordering and duplicates |
| **Snowflake Streams** | Changes since last consumption | Within Snowflake only. Staleness risk |
| **Full compare / hash diff** | Compare source vs target hashes | Expensive. Used when the source lacks timestamps |
| **Partition overwrite** | Rebuild affected days entirely | Simple, idempotent. Good for event data |

**Handling deletes:** log-based CDC with delete events → soft delete (`is_deleted`, `deleted_at`) in curated layer; or periodic full key comparison (anti-join source keys vs target) to detect hard deletes.

**Orchestration tools:** Airflow (most common), Dagster, Prefect, dbt Cloud jobs, Azure Data Factory, AWS Step Functions/MWAA, Snowflake Tasks. Know: DAG, operators/sensors, retries, SLAs, backfills (`catchup`), idempotent tasks, passing only metadata between tasks (not data), and Airflow + dbt integration (e.g., Cosmos, `dbt build` per model group).

**ETL tools named in the JD:**
- **Informatica (PowerCenter / IDMC/IICS)**: mappings, sessions, workflows, pushdown optimization (ELT pushdown to Snowflake), Snowflake connector.
- **Talend**: jobs/components, tSnowflake components, ELT components, context variables.
- **dbt**: SQL-first ELT inside Snowflake (section 14).
- **Custom frameworks**: Python + Snowflake connector/Snowpark, metadata-driven (config tables / YAML describing sources, targets, keys, load type).

**Metadata-driven framework (great senior answer):**
```
config table / YAML: source_system | source_object | target_table | load_type (full/incremental/cdc)
                     | business_key | watermark_col | schedule | quality_rules | owner | pii_flag
generic procedures:  ingest → validate → merge → audit (one code path, N tables)
control tables:      etl_control (watermarks), etl_audit (runs, row counts, status, duration, errors)
```
> Benefit: onboarding a new table is a config change plus a PR, not new code. Consistent logging and quality checks everywhere.

### Q&A

**🟢 Q1. Walk me through a pipeline you built end to end.** (Prepare your own with this structure.)
> **Situation/Scale:** sources, volume (rows/day, TB), latency SLA.
> **Ingestion:** e.g., Fivetran/Snowpipe from S3 into RAW (VARIANT + metadata).
> **Transformation:** dbt staging → intermediate → marts. Incremental models with merge and a 3-day lookback.
> **Orchestration:** Airflow DAG with `dbt build` by tag/layer, retries, SLA alerts.
> **Quality:** dbt tests (unique, not_null, relationships, freshness), row-count reconciliation against the source.
> **Governance:** PII tagging and masking, RBAC roles per layer.
> **Ops:** monitoring dashboard (freshness, failures, credits), runbook.
> **Outcome:** quantify. "Cut runtime from 3h to 25m, reduced credits 40%, SLA 99.5%."

**🟡 Q2. How do you design incremental loads when the source has no reliable timestamp?**
> Options in order of preference: get log-based CDC (Debezium, Fivetran, database change tracking); use a monotonically increasing ID for inserts plus periodic hash compare for updates; compute row hashes in staging and MERGE only rows whose hash differs; for small tables, just full reload (cheap in Snowflake), using CTAS + SWAP for atomicity.

**🟡 Q3. How do you handle schema changes from the source?**
> Land raw as VARIANT or with schema evolution enabled so ingestion doesn't break. In dbt, use `on_schema_change` (`append_new_columns` / `sync_all_columns`) for incremental models where appropriate. Have **data contracts** on published models so breaking changes fail in CI rather than in production. Monitor for new or removed columns via `INFORMATION_SCHEMA` diff alerts, and communicate with source owners (in a federated model this is part of the contract).

**🟡 Q4. How do you backfill two years of history without disrupting production?**
> Run the backfill in **batches** (by month) with a parameterized job, on a **separate warehouse** sized for it, writing to the target with idempotent logic (delete+insert per partition or MERGE). Or build into a clone/new table and `SWAP` at the end. Validate counts and aggregates per batch against the source. Schedule outside peak hours. Tell downstream consumers. dbt: `--full-refresh` on an isolated target, or microbatch incremental strategy with `--event-time-start/--event-time-end`.

**🟡 Q5. How do you make a pipeline resilient to failures?**
> Idempotent steps, retries with backoff for transient errors, checkpointing (watermarks committed only after success), atomic swaps for table rebuilds, quarantine tables for bad records instead of failing the whole load (with thresholds), alerting to Slack/PagerDuty, and a runbook. Plus Time Travel as a safety net for bad writes.

**🔴 Q6. Batch vs streaming: how do you decide?**
> Driven by the business latency requirement and cost. Most analytics is fine with hourly or daily batch. Use near-real-time (Snowpipe ~1 min, Snowpipe Streaming seconds, dynamic tables with minute-level lag) only where decisions actually need it (fraud, operations, inventory). Streaming adds complexity: ordering, duplicates, late data, exactly-once semantics, monitoring. I'd often do "micro-batch with Snowpipe + dynamic tables" as a sweet spot.

**🔴 Q7. Exactly-once processing with Kafka into Snowflake?**
> Kafka gives at-least-once by default. The Snowflake Kafka connector with Snowpipe Streaming tracks offsets per channel to avoid duplicates on restarts. Still, I design for duplicates: keep the Kafka metadata (topic, partition, offset) in the raw table and dedupe downstream with `QUALIFY ROW_NUMBER() OVER (PARTITION BY topic, partition, offset …) = 1` or by the business event ID.

**🔴 Q8. Migrating from Informatica / Teradata / on-prem to Snowflake: how would you approach it?**
> 1. **Assess**: inventory objects, jobs, dependencies, data volumes, SLAs, usage (decommission unused).
> 2. **Design target**: layers, RBAC, warehouses, naming, dbt project structure, CI/CD.
> 3. **Migrate schema**: convert DDL (data types, no indexes, no distribution keys), and use tools (SnowConvert) to translate SQL/procedures.
> 4. **Migrate data**: historical bulk unload to cloud storage → COPY. Incremental sync during parallel run.
> 5. **Re-platform pipelines**: lift-and-shift with Informatica pushdown to Snowflake for speed, or re-engineer into ELT/dbt for long-term value. Prioritize by business value.
> 6. **Validate**: row counts, checksums, aggregate comparisons, report reconciliation, in a parallel run.
> 7. **Cut over** by domain/wave, with rollback plan. Decommission legacy. Optimize cost after.

---

## 14. dbt

### Core concepts

**What dbt is:** the "T" in ELT. You write `SELECT` statements (models) plus YAML config. dbt compiles Jinja → SQL, builds a **DAG** from `ref()`/`source()`, runs it in dependency order against Snowflake, and adds testing, documentation, lineage, and environments. **dbt Core** (open source CLI), **dbt Cloud / dbt platform** (managed IDE, jobs, CI, Explorer, Semantic Layer, Mesh), and the newer Rust-based **dbt Fusion engine** (faster parsing, SQL comprehension). Snowflake also offers **dbt Projects on Snowflake** to run dbt natively in the account.

**Project structure (best practice):**
```
models/
  staging/<source>/        stg_<source>__<entity>.sql   (views; 1:1 with source; rename, cast, light clean)
                           _<source>__sources.yml, _<source>__models.yml
  intermediate/<domain>/   int_<entity>_<verb>.sql      (ephemeral/views; joins, business logic)
  marts/<domain>/          fct_<process>.sql, dim_<entity>.sql (tables/incremental)
snapshots/   seeds/   macros/   tests/   analyses/
dbt_project.yml   packages.yml   profiles.yml (usually outside the repo, or env vars)
```

**Key functions & concepts**
- `{{ ref('model') }}`: dependency + environment-aware relation name
- `{{ source('src', 'table') }}`: declared raw sources (enables freshness + lineage)
- `{{ config(...) }}` in-model, or in `dbt_project.yml` by folder (`+materialized: view`)
- `{{ this }}`: the current model's relation (used in incremental logic)
- `{{ var('x') }}`, `{{ env_var('X') }}`, `target.name`, `target.schema`
- **Macros**: reusable Jinja functions. **Packages**: `dbt_utils`, `dbt_expectations`, `codegen`, `dbt_project_evaluator`, `audit_helper`, `elementary`
- **Hooks**: `pre_hook`, `post_hook`, `on-run-start`, `on-run-end` (e.g., grants, query tags, audit)
- **Commands**: `dbt run`, `test`, `build` (run+test+snapshot+seed in DAG order, stopping downstream on test failure), `seed`, `snapshot`, `compile`, `docs generate/serve`, `source freshness`, `deps`, `debug`, `ls`, `clone`, `retry`, `show`

**Materializations**

| Materialization | What | Use |
|---|---|---|
| `view` | CREATE VIEW | Staging, light logic, always fresh, no storage |
| `table` | CREATE OR REPLACE TABLE AS | Marts with heavy logic, small–medium data |
| `incremental` | Insert/merge only new/changed rows | Large fact tables, event data |
| `ephemeral` | Inlined as CTE, not built | Small reusable logic (don't overuse: hard to debug) |
| `materialized_view` | Snowflake MV | Limited use |
| `dynamic_table` | Snowflake dynamic table with `target_lag` | Near-real-time declarative pipelines |
| `snapshot` | SCD Type 2 | History of mutable sources |

> On Snowflake, dbt **tables are transient by default** (`transient: true`), so no Fail-safe. Set `+transient: false` for tables needing Fail-safe.

**Incremental models (deep dive)**
```sql
{{ config(
    materialized = 'incremental',
    unique_key = 'order_id',
    incremental_strategy = 'merge',          -- merge (default on Snowflake) | append | delete+insert | insert_overwrite | microbatch
    on_schema_change = 'append_new_columns',  -- ignore (default) | fail | append_new_columns | sync_all_columns
    cluster_by = ['order_date'],
    merge_update_columns = ['status', 'amount', 'updated_at'],
    incremental_predicates = ["DBT_INTERNAL_DEST.order_date >= dateadd(day, -7, current_date)"]  -- prune the target scan
) }}

select order_id, customer_id, order_date, status, amount, updated_at
from {{ ref('stg_shop__orders') }}

{% if is_incremental() %}
  -- lookback window to catch late-arriving updates
  where updated_at >= (select dateadd(day, -3, max(updated_at)) from {{ this }})
{% endif %}

qualify row_number() over (partition by order_id order by updated_at desc) = 1
```
- `is_incremental()` is true only if the target exists, the model is incremental, and it isn't `--full-refresh`.
- Strategies on Snowflake: **merge** (needs `unique_key`), **append** (no dedupe), **delete+insert** (delete matching keys then insert, good for batches by key/partition), **insert_overwrite** (in newer adapter versions, replaces the table contents. Check your adapter version), **microbatch** (dbt 1.9+: processes by `event_time` batches, with built-in backfill and retry of failed batches).
- `incremental_predicates` limit the merge's target scan, a big performance win on huge tables.
- Duplicate keys in the source batch → MERGE error ("Duplicate row detected during DML action") unless `ERROR_ON_NONDETERMINISTIC_MERGE = FALSE`. Always dedupe.
- `--full-refresh` rebuilds from scratch. Use `full_refresh: false` config to protect huge tables from accidental rebuilds.

**Snapshots (SCD2)**
```yaml
# snapshots/customers_snapshot.yml  (dbt 1.9+ YAML config; older versions use {% snapshot %} blocks)
snapshots:
  - name: customers_snapshot
    relation: source('crm', 'customers')
    config:
      schema: snapshots
      unique_key: customer_id
      strategy: timestamp          # or 'check' with check_cols: [email, city] or 'all'
      updated_at: updated_at
      hard_deletes: invalidate     # ignore | invalidate | new_record
```
> Adds `dbt_scd_id`, `dbt_updated_at`, `dbt_valid_from`, `dbt_valid_to` (NULL = current, or configurable `dbt_valid_to_current`). **Timestamp** strategy is preferred when there's a reliable `updated_at`. **Check** compares columns (handles sources without timestamps, more expensive). Snapshots should select from **sources** (raw) with minimal transformation and are rarely full-refreshed, since history can't be regenerated.

**Tests**
```yaml
models:
  - name: fct_orders
    description: "One row per order. Grain: order_id."
    config:
      contract: {enforced: true}           # model contract: column names & data types must match
    columns:
      - name: order_id
        data_type: varchar
        data_tests: [unique, not_null]      # 'tests:' in older versions; 'data_tests:' from 1.8
      - name: customer_id
        data_type: varchar
        data_tests:
          - relationships: {to: ref('dim_customer'), field: customer_id}
      - name: status
        data_type: varchar
        data_tests:
          - accepted_values: {values: ['placed','shipped','delivered','returned']}
      - name: amount
        data_type: number(18,2)
        data_tests:
          - dbt_utils.accepted_range: {min_value: 0}
    data_tests:
      - dbt_utils.unique_combination_of_columns: {combination_of_columns: [order_id]}
```
- **Generic tests** (built-in: `unique`, `not_null`, `accepted_values`, `relationships`). Custom generic tests are macros with `{% test %}`.
- **Singular tests**: SQL files in `tests/` returning failing rows.
- **Unit tests** (dbt 1.8+): mock inputs and expected outputs for a model's logic, run in CI.
- Test configs: `severity: warn|error`, `warn_if`, `error_if` thresholds, `where`, `store_failures: true` (store failing rows in a table for debugging), `limit`.
- **Source freshness:**
  ```yaml
  sources:
    - name: shop
      database: raw
      loaded_at_field: _loaded_at
      freshness: {warn_after: {count: 6, period: hour}, error_after: {count: 24, period: hour}}
      tables: [{name: orders}, {name: customers}]
  ```

**Environments & schemas**
- `profiles.yml` targets: `dev` (schema `dbt_<user>`), `ci` (schema `dbt_ci_pr_<n>`), `prod`. Use env vars and key-pair auth for secrets.
- `generate_schema_name` macro: default concatenates `target_schema + '_' + custom_schema`. Override it so prod uses clean custom schemas (`marts`) while dev stays isolated (`dbt_jdoe_marts`).
- `generate_database_name`, `generate_alias_name` similarly.

**Node selection (essential for CI and orchestration)**
```
dbt build --select stg_shop__orders          # one model
dbt build --select +fct_orders               # model + all upstream
dbt build --select fct_orders+               # model + all downstream
dbt build --select @fct_orders               # model, descendants, and descendants' ancestors
dbt build --select tag:hourly                # by tag
dbt build --select path:models/marts/finance
dbt build --select source:shop+              # everything downstream of a source
dbt build --select state:modified+ --defer --state ./prod-artifacts   # Slim CI
dbt build --exclude tag:deprecated
dbt retry                                     # rerun from point of failure
dbt build --select result:error+ --state ./target   # rerun failures + downstream
```

**Slim CI**: compare against production's `manifest.json`. Build only `state:modified+` models. `--defer` makes unmodified upstream refs resolve to **production** objects, so CI doesn't rebuild the whole project. (dbt Cloud does this natively. dbt 1.6+ `dbt clone` can zero-copy clone prod models into the CI schema on Snowflake.)

**Snowflake-specific dbt configs:** `snowflake_warehouse` (per-model warehouse), `cluster_by`, `automatic_clustering`, `transient`, `copy_grants`, `query_tag`, `secure` (views), `target_lag` (dynamic tables), `grants: {select: ['reporter']}`, `merge_exclude_columns`, `tmp_relation_type`.

**dbt Mesh / governance features (senior):** `groups` and `access` (`private`, `protected`, `public`), **model contracts**, **model versions** (`v1`, `v2` with deprecation dates), cross-project `ref('project', 'model')`. These map exactly to a **federated** setup: domains publish public, contracted, versioned models.

**Other features:** `exposures` (downstream dashboards in lineage), Semantic Layer/MetricFlow (`semantic_models`, `metrics`), `docs` blocks, `persist_docs` (pushes descriptions to Snowflake comments), `meta` (owner, PII flags), `analyses`, seeds (small static CSVs only).

### Q&A

**🟢 Q1. What is dbt and why use it?**
> dbt is a transformation framework that lets you build warehouse models as modular `SELECT` statements with Jinja. It handles dependency ordering via `ref()`, materializations, testing, documentation, and lineage. It brings software engineering practices (Git, code review, CI/CD, environments, reusable macros) to SQL transformations. With Snowflake, it's the standard ELT pattern.

**🟢 Q2. `ref()` vs `source()`?**
> `source()` references raw tables loaded by ingestion tools, declared in YAML (enables freshness checks and lineage from raw). `ref()` references other dbt models. It builds the DAG and resolves to the right database/schema per environment. You should never hardcode table names.

**🟢 Q3. Explain dbt materializations and when to use each.**
> View for staging (cheap, always fresh). Table for marts with heavy logic and moderate size. Incremental for large, append-heavy or updatable facts where rebuilding is too slow or expensive. Ephemeral for small reusable CTE logic. Snapshot for SCD2 history. On Snowflake, also dynamic tables for near-real-time with target lag.

**🟡 Q4. How do incremental models work? What problems have you hit?**
> On the first run (or `--full-refresh`) dbt builds the full table. Afterwards, `is_incremental()` filters the source to new or changed rows, dbt creates a temp relation, and applies the strategy (merge by `unique_key` on Snowflake by default). Problems and fixes:
> - **Late-arriving data** missed by a strict `>` watermark → lookback window.
> - **Duplicate keys** in the batch → MERGE fails → dedupe with `QUALIFY`.
> - **Schema changes** → `on_schema_change`.
> - **Slow merges on huge targets** → `incremental_predicates` and clustering on the date.
> - **Drift from logic changes** → planned full refresh or targeted backfill.
> - **Hard deletes** not captured → soft-delete flags from CDC or periodic reconciliation.

**🟡 Q5. dbt `run` vs `build`?**
> `run` executes models only. `build` runs models, tests, seeds, and snapshots in DAG order, and **skips downstream nodes when an upstream test fails**. That prevents bad data from propagating. I use `build` in production and CI.

**🟡 Q6. How do snapshots work? Timestamp vs check strategy?**
> Snapshots implement SCD2 on a mutable source. Each run compares the source to the snapshot table: changed rows get `dbt_valid_to` set and a new version inserted. **Timestamp** strategy uses an `updated_at` column (reliable and efficient). **Check** strategy compares specified columns (when there's no reliable timestamp, more expensive). `hard_deletes: invalidate`/`new_record` handles deleted source rows.

**🟡 Q7. What tests do you put on every model?**
> Primary key `unique` + `not_null` (grain test) on every model. `relationships` on foreign keys in marts. `accepted_values` on status/enum fields. Source freshness on all sources. Then business-rule singular tests (e.g., revenue ≥ 0, order totals = sum of lines), `dbt_expectations` for distributions or row-count ranges. Unit tests for complex logic. Severity: `error` for things that make data wrong, `warn` for anomalies, with `store_failures` for debugging.

**🟡 Q8. How do you manage dev/CI/prod environments in dbt with Snowflake?**
> Separate targets in `profiles.yml` with key-pair auth via env vars. Developers build into personal schemas (`dbt_<name>`) in a DEV database, often on zero-copy clones of prod data or with `--defer` to prod. CI builds into a PR-specific schema. Prod deploys via orchestrator/dbt Cloud jobs with a service role. The `generate_schema_name` override keeps prod schema names clean. Different warehouses and roles per environment.

**🟡 Q9. How do you write a reusable macro? Give an example.**
```sql
-- macros/cents_to_dollars.sql
{% macro cents_to_dollars(col, precision=2) %}
    round({{ col }} / 100.0, {{ precision }})
{% endmacro %}

-- macros/grant_select.sql  (run as on-run-end hook)
{% macro grant_select(role) %}
  {% for schema in schemas %}
    grant usage on schema {{ schema }} to role {{ role }};
    grant select on all tables in schema {{ schema }} to role {{ role }};
  {% endfor %}
{% endmacro %}
```
> (Modern dbt has the `grants` config, which is preferred over hand-rolled grant macros.)

**🔴 Q10. What is Slim CI and how do you implement it with dbt Core?**
> Production job uploads `manifest.json` (artifacts) to S3 after each run. The CI job on a PR downloads it and runs `dbt build --select state:modified+ --defer --state prod_artifacts --target ci`. Only changed models and their downstream get built (into a PR schema), and unchanged parents resolve to prod. That makes CI fast and cheap. Optionally `dbt clone` to zero-copy prod tables into the CI schema for incremental models. Drop the PR schema after merge.

**🔴 Q11. How would you structure dbt for many federated teams?**
> Either one monorepo with domain folders, CODEOWNERS, and groups/access controls, or **dbt Mesh**: multiple projects (one per domain plus a platform/core project), with cross-project refs to `public`, **contracted**, and **versioned** models. The platform team provides a shared package of macros, tests, and conventions (naming, `generate_schema_name`, query tags, audit hooks), CI templates, and `dbt_project_evaluator` to enforce standards. Each domain owns its project's deployment and SLAs.

**🔴 Q12. What are model contracts and versions?**
> A contract (`contract: enforced: true`) declares column names, data types, and optional constraints. dbt fails the build if the model's output doesn't match, so breaking changes are caught before consumers see them. Versions let you publish `v2` of a model with breaking changes while `v1` stays available until a deprecation date, which gives downstream teams a migration window. Together they're how you do data contracts in dbt.

**🔴 Q13. dbt performance tuning on Snowflake?**
> - Choose the right materializations (incremental for big facts. Don't make big things views that get re-queried).
> - `incremental_predicates` + `cluster_by` on large merges.
> - Per-model `snowflake_warehouse` to give heavy models a bigger warehouse and keep the rest small.
> - Increase `threads` for parallelism (warehouse concurrency permitting).
> - Avoid deep ephemeral chains (they make giant SQL). Materialize intermediates.
> - Use `query_tag` to find slow models in `QUERY_HISTORY`, and check `run_results.json` timings.
> - Slim CI and `dbt retry` to avoid rebuilding everything.

**🔴 Q14. How do you handle a breaking change in a widely used model?**
> Add a new version (`v2`) with the change, keep `v1` with a deprecation date, notify consumers (exposures tell you who's affected), monitor usage via `ACCESS_HISTORY`, then remove `v1`. For non-versioned projects: add new columns rather than renaming, deprecate the old ones over a period, communicate, and enforce with contracts in CI.

---

## 15. Git

### Core concepts

- **Distributed VCS.** Working directory → staging area (index) → local repo → remote.
- **Core commands:** `clone, status, add, commit, push, pull (= fetch + merge/rebase), fetch, branch, checkout/switch, merge, rebase, log, diff, stash, tag, reset, revert, cherry-pick, restore, blame, reflog`.

**Branching strategies**

| Strategy | How | Good for |
|---|---|---|
| **GitHub Flow / trunk-based** | Short-lived feature branches off `main`, PR, merge, deploy | Most dbt/data teams. Continuous delivery |
| **GitFlow** | `main`, `develop`, `feature/*`, `release/*`, `hotfix/*` | Scheduled releases, strict environments |
| **Environment branches** | `dev` → `qa` → `main` promotions | Common in data teams with separate environments (risk: drift between branches) |

**Merge vs rebase vs squash**
- **Merge**: preserves history, adds a merge commit. Safe for shared branches.
- **Rebase**: replays your commits on top of the target for linear history. **Never rebase shared/public branches** (rewrites history).
- **Squash merge**: one clean commit per PR on `main`.

**Undoing things**
- `git restore <file>`: discard working changes
- `git reset --soft HEAD~1`: undo commit, keep changes staged. `--mixed` (default): keep unstaged. `--hard`: discard (dangerous)
- `git revert <sha>`: new commit that undoes a commit (**safe for shared branches**: use this in prod)
- `git reflog`: recover "lost" commits after a bad reset
- `git cherry-pick <sha>`: apply a specific commit (e.g., hotfix to a release branch)
- `git stash` / `stash pop`: shelve work temporarily

### Q&A

**🟢 Q1. What's your Git workflow for data pipelines?**
> Trunk-based: branch from `main` (`feature/JIRA-123-add-orders-fact`), commit small logical changes with clear messages, push, open a PR. CI runs lint, compile, and dbt build/test on changed models. A teammate reviews (CODEOWNERS for the domain). Squash-merge. CD deploys to prod. Protected `main` (required reviews and green checks, no direct pushes).

**🟢 Q2. `git merge` vs `git rebase`?**
> Merge combines histories and creates a merge commit, preserving context. Rebase rewrites my branch's commits on top of the latest main for linear history. I rebase my own local feature branch to keep it current and clean, but never rebase branches others are using. I merge (or squash-merge) into main via PR.

**🟢 Q3. `git reset` vs `git revert`?**
> Reset moves the branch pointer back (rewrites history). Fine locally, dangerous on shared branches. Revert creates a new commit that undoes a previous commit's changes. It's safe and auditable, and the right way to roll back something already on main.

**🟡 Q4. How do you resolve a merge conflict?**
> `git fetch`, then `git rebase origin/main` (or merge). Git marks conflicting hunks (`<<<<<<<`, `=======`, `>>>>>>>`). Understand both changes (talk to the other author if needed), edit to the correct combined version, `git add`, `git rebase --continue`, then **re-run tests** (dbt compile/build) because a textually clean resolution can still be logically wrong.

**🟡 Q5. What makes a good PR for a data change?**
> Small and focused. Description with the why, the what, and the impact (affected models, downstream exposures, full refresh needed?). Evidence: CI results, row-count or before/after comparison (e.g., `audit_helper.compare_relations`). Tests added. Docs updated. Deployment notes (backfill, ordering). A reviewer checklist: naming, grain, tests, performance, PII handling.

**🟡 Q6. What should never be committed?**
> Secrets (passwords, private keys, tokens, `profiles.yml` with credentials), `.env` files, large data files, generated artifacts (`target/`, `dbt_packages/`, `logs/`). Use `.gitignore`, secret scanning (GitHub secret scanning, `gitleaks`, pre-commit hooks). If a secret is committed: **rotate it immediately** (removing it from history isn't enough), then purge it with `git filter-repo`/BFG.

**🔴 Q7. How do you handle a hotfix in production when main has unreleased changes?**
> Trunk-based with continuous deploy: fix forward on a short branch, fast PR, deploy. If main contains unreleasable work: branch from the prod release **tag**, apply the fix, deploy from that branch, and cherry-pick (or merge) the fix back to main. Feature flags or dbt `enabled` configs help keep unreleased work dark.

**🔴 Q8. Monorepo vs multi-repo for data?**
> Monorepo: easy cross-team changes, a single CI, consistent standards. Needs CODEOWNERS and path-based CI to scale. Multi-repo (e.g., per domain with dbt Mesh): clear ownership and independent deploys, but cross-repo changes and version coordination are harder. For federated teams I'd lean toward a dbt Mesh multi-project setup with a shared standards package. Small teams: monorepo.

---

## 16. CI/CD for Data Pipelines

### Core concepts

**CI (Continuous Integration):** every PR automatically validated.
**CD (Continuous Delivery/Deployment):** merged changes automatically deployed to environments.

**What a data CI pipeline should check**
1. **Lint / format**: SQLFluff (dbt templater), `yamllint`, pre-commit hooks, Black/Ruff for Python
2. **Compile/parse**: `dbt deps && dbt compile` (or `dbt parse`). Catches Jinja/ref errors
3. **Standards**: `dbt_project_evaluator` (naming, test coverage, docs), `dbt-checkpoint` (every model has description/tests/owner)
4. **Build & test changed models**: `dbt build --select state:modified+ --defer --state prod/` into an isolated PR schema
5. **Unit tests** (dbt unit tests, pytest for Python)
6. **Data diff / impact**: compare PR outputs vs prod (`audit_helper`, Datafold, `dbt-audit-helper`), and contract checks
7. **Security**: secret scanning, dependency scanning
8. **Cleanup**: drop the PR schema after merge/close

**CD pipeline**
1. Merge to `main` → deploy to **QA/staging** → automated tests → (optional manual approval) → **prod**
2. Deploy database objects (DDL) with a migration tool, deploy dbt (`dbt build` with prod target), publish docs, upload artifacts (`manifest.json`) for the next Slim CI
3. Notify. Tag the release

**Managing Snowflake objects as code (DDL that dbt doesn't own)**

| Tool | Approach | Use for |
|---|---|---|
| **Terraform** (Snowflake provider, now Snowflake-maintained) | Declarative IaC with state | Account objects: warehouses, roles, grants, users, databases, integrations, resource monitors |
| **schemachange** | Imperative versioned scripts (`V1.1__create_x.sql`, `R__` repeatable, `A__` always), change history table | DDL, procs, tasks, streams, pipes |
| **Flyway / Liquibase** | Versioned migrations | Same as above, cross-DB teams |
| **Snowflake CLI + `CREATE GIT REPOSITORY` + `EXECUTE IMMEDIATE FROM`** | Native: Snowflake pulls from Git and executes (Jinja-templated) SQL files | Native deployments without extra tools |
| **Snowflake DCM / declarative `CREATE OR ALTER`** | Declarative object definitions | Newer native option. Verify maturity |
| **dbt** | Models, tests, snapshots, grants | Transformation layer |
| **Permifrost / Titan** | RBAC as code | Roles/grants |

**Example: GitHub Actions dbt Slim CI**
```yaml
name: dbt-ci
on:
  pull_request:
    branches: [main]
    paths: ['models/**', 'macros/**', 'snapshots/**', 'tests/**', 'dbt_project.yml']
jobs:
  ci:
    runs-on: ubuntu-latest
    env:
      SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
      SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_CI_USER }}
      SNOWFLAKE_PRIVATE_KEY: ${{ secrets.SNOWFLAKE_CI_PRIVATE_KEY }}   # key-pair auth
      DBT_CI_SCHEMA: dbt_ci_pr_${{ github.event.pull_request.number }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: {python-version: '3.11'}
      - run: pip install dbt-snowflake sqlfluff sqlfluff-templater-dbt
      - run: dbt deps
      - name: Lint changed SQL
        run: |
          git fetch origin main
          git diff --name-only origin/main...HEAD -- 'models/*.sql' | xargs -r sqlfluff lint
      - name: Get prod manifest
        run: aws s3 cp s3://dbt-artifacts/prod/manifest.json prod-artifacts/manifest.json
      - name: Slim CI build
        run: dbt build --select state:modified+ --defer --state prod-artifacts --target ci --fail-fast
```
```yaml
# Separate workflow on PR close: drop schema
#   dbt run-operation drop_schema --args '{schema: dbt_ci_pr_123}'
```

**Example: CD on merge**
```yaml
on: {push: {branches: [main]}}
jobs:
  deploy:
    environment: production          # GitHub environment with required reviewers
    steps:
      - checkout, setup, deps …
      - run: schemachange deploy -f migrations/ --config-folder . -c META.SCHEMACHANGE.CHANGE_HISTORY
      - run: dbt build --target prod --select state:modified+ --state prod-artifacts   # or full scheduled build
      - run: aws s3 cp target/manifest.json s3://dbt-artifacts/prod/manifest.json
```

**Deployment & release strategies**
- **Blue/green with zero-copy clone + SWAP**: build the new version into a clone (`DB_PROD_GREEN`), test, then `ALTER DATABASE … SWAP WITH …` (or schema/table swap). Instant, atomic cutover. Easy rollback by swapping back.
- **Write-Audit-Publish (WAP)**: write to a hidden/staging location → audit (tests) → publish (swap/rename) only if tests pass. dbt `build` + swap pattern.
- **Rollback**: `git revert` + redeploy for code. Time Travel/clone to restore data. Keep migrations reversible where possible.
- **Feature flags**: dbt `enabled: "{{ var('enable_x', false) }}"`.

**Environment strategy in Snowflake**
- Separate databases per environment (`DEV_`, `QA_`, `PROD_`) or separate accounts (stronger isolation. Use replication or shares to get prod data).
- Dev and QA data from **zero-copy clones of prod** (masking policies still apply. Clone grants handled carefully) or sampled/synthetic data for PII-heavy domains.
- Separate service users and roles per environment. CI can never write to prod.

### Q&A

**🟢 Q1. What is CI/CD and why does it matter for data pipelines?**
> CI automatically validates every change (lint, compile, test against realistic data) before merge. CD automatically and consistently deploys merged changes across environments. For data, it prevents broken models, bad data, and manual deployment errors from reaching production, makes changes auditable and reversible, and lets many engineers (or federated teams) contribute safely.

**🟡 Q2. Describe a CI/CD pipeline you've built for Snowflake + dbt.**
> (Adapt to your experience.) GitHub with protected main. On PR, GitHub Actions ran SQLFluff, `dbt compile`, then Slim CI: `dbt build --select state:modified+ --defer` into a PR-specific schema using a CI service user with key-pair auth and an XS warehouse. It posted results to the PR, and a cleanup job dropped the schema on close. On merge: schemachange applied DDL migrations (procs, tasks, stages), then dbt deployed to prod via Airflow/dbt Cloud jobs, and the manifest was uploaded for the next Slim CI. Terraform managed warehouses, roles, and grants in a separate repo with plan-on-PR and apply-on-merge. Outcome: CI ~8 minutes, zero manual deployments, failed deploys dropped to near zero.

**🟡 Q3. How do you test data pipelines in CI without production data exposure?**
> Options: zero-copy clone of prod into a CI database with masking policies in force (the CI role lacks PII-unmask privileges); `--defer` to read prod objects through a restricted role; sampled or synthetic datasets; dbt unit tests with mocked inputs for logic. Never give the CI role broad prod access or write privileges.

**🟡 Q4. How do you deploy Snowflake objects that dbt doesn't manage (procs, tasks, pipes, stages, roles)?**
> Account-level objects (warehouses, roles, grants, integrations) go through Terraform. Database-level procedural objects go through schemachange (versioned migrations tracked in a change history table) or Snowflake's native Git repository integration with `EXECUTE IMMEDIATE FROM`. All in Git with PR review, all promoted through environments by the pipeline. Never manual changes in prod (detect drift periodically).

**🟡 Q5. How do you roll back a bad deployment?**
> Code: `git revert` the merge commit and redeploy. Data: if bad data was written, use Time Travel (`CLONE … BEFORE(STATEMENT => …)` or `AT(TIMESTAMP => …)`) and `SWAP`. Or, with blue/green, swap back to the previous database or schema. Then rerun affected downstream models and post-mortem. Design to minimize it: WAP pattern, tests that block publishing, small incremental releases.

**🔴 Q6. What's the Write-Audit-Publish pattern and how do you implement it in Snowflake?**
> Write new data to a non-consumer-facing location, audit it with automated quality checks, and publish only if it passes. In Snowflake: build into a staging schema or clone, run dbt tests / Data Metric Functions, then `ALTER TABLE prod.x SWAP WITH staging.x` (atomic, metadata-only). On failure, consumers keep the last good version and you get alerted. Prevents bad data from ever being visible.

**🔴 Q7. How would you set up CI/CD for multiple federated teams?**
> A **platform-provided reusable CI/CD template** (GitHub reusable workflows/composite actions) that every domain repo or project uses: lint rules, required tests and docs (`dbt_project_evaluator` thresholds), contract checks, Slim CI, security scans, and deployment steps. Domain teams own their pipelines but inherit the guardrails. CODEOWNERS require domain plus platform review for shared or core models. Separate service users and warehouses per domain for isolation and cost attribution. Central observability (dbt artifacts → Snowflake tables → dashboard).

**🔴 Q8. How do you detect and prevent drift between Git and Snowflake?**
> Everything deployed only by pipelines (humans have read-only roles in prod). Run `terraform plan` on a schedule to detect drift in account objects. Compare `INFORMATION_SCHEMA`/`GET_DDL` output against repo definitions for procedural objects. Audit `QUERY_HISTORY` for DDL run by non-service users. Alert on any manual change.

---

## 17. Data Quality, Governance & Metadata

### Core concepts

**Data quality dimensions:** accuracy, completeness, consistency, timeliness/freshness, validity (format/range), uniqueness, integrity (referential).

**Where checks live**
- **At ingestion**: schema validation, file-level checks, row counts vs source manifest, quarantine bad records
- **In transformations**: dbt tests (PK, FK, accepted values, ranges, business rules), unit tests, contracts
- **At publishing**: WAP gates, reconciliation with source/finance totals
- **Continuous monitoring**: freshness, volume anomalies, distribution drift, schema changes
- **Snowflake-native**: **Data Metric Functions (DMFs)**: system DMFs (`SNOWFLAKE.CORE.NULL_COUNT`, `DUPLICATE_COUNT`, `FRESHNESS`, `ROW_COUNT`, `UNIQUE_COUNT`, …) and custom DMFs, scheduled on tables with `DATA_METRIC_SCHEDULE`, results in `DATA_QUALITY_MONITORING_RESULTS`. Can set expectations and alerts. Plus **Snowflake Alerts** (`CREATE ALERT … IF (EXISTS (…)) THEN CALL SYSTEM$SEND_EMAIL(...)`)
- **Tools**: dbt tests, Great Expectations, Soda, Monte Carlo, Elementary, Anomalo, Bigeye

```sql
ALTER TABLE core.orders SET DATA_METRIC_SCHEDULE = '60 MINUTE';
ALTER TABLE core.orders ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (customer_id);
ALTER TABLE core.orders ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (order_id);
```

**Governance pillars:** ownership and stewardship (RACI), data catalog and discovery, classification and PII tagging, access policies (RBAC, masking, row access), lineage, retention and lifecycle, auditing (`ACCESS_HISTORY`, `LOGIN_HISTORY`), standards (naming, modeling, documentation), data contracts, compliance (GDPR/CCPA right-to-be-forgotten, HIPAA, SOX).

**Metadata management**
- **Technical**: schemas, types, lineage (dbt DAG, `ACCESS_HISTORY` / Snowflake lineage in Snowsight), query stats
- **Business**: definitions, glossary, owners, SLAs (dbt descriptions, `meta`, `persist_docs` → Snowflake comments)
- **Operational**: run logs, freshness, quality results, costs (dbt artifacts, audit tables)
- **Catalogs**: Snowflake Horizon catalog, Alation, Collibra, Atlan, DataHub, Unity/Open Catalog for Iceberg

**Data contracts:** an agreement between a producer and consumers that defines schema, semantics, quality expectations, SLAs (freshness), ownership, and change process. Enforced by dbt model contracts + tests + CI, and versioning for breaking changes.

### Q&A

**🟡 Q1. How do you ensure data integrity across pipelines?** (JD bullet)
> Layered checks: ingestion reconciliation (source count/checksum vs loaded), PK uniqueness and not-null on every model, referential integrity tests between facts and dims, business rule tests, freshness SLAs, and periodic reconciliation with systems of record (e.g., finance totals). Idempotent loads (MERGE, dedupe) prevent duplicates. Transactions or swaps prevent partial writes. Failures block publishing (dbt `build`, WAP). Alerts go to owners.

**🟡 Q2. How do you ensure data security across workflows?** (JD bullet)
> Least-privilege RBAC with functional/access roles, service users with key-pair auth and network policies, secrets in a vault, PII classification and tag-based masking, row access policies for regional restrictions, secure views for sharing, encryption (default) plus Tri-Secret Secure if required, PrivateLink for network isolation, audit via `ACCESS_HISTORY`, and no PII in dev unless masked. Everything is code-reviewed.

**🟡 Q3. How would you implement GDPR "right to be forgotten" in Snowflake?**
> Maintain a deletion-request table. A scheduled process deletes or anonymizes the subject's rows across all tables tagged as containing personal data (discoverable via tags or classification). Remember Time Travel and Fail-safe retain data up to 90 + 7 days, so document retention in policy or minimize retention on PII tables. Snapshots/SCD2 history and downstream marts must be handled too. Prefer pseudonymization (a tokenized ID in facts, PII only in one dimension) so deletion touches few tables. Log the completion for audit.

**🔴 Q4. How do you set up data governance and best practices as a senior engineer?** (JD bullet)
> Start with what hurts: incidents, untrusted numbers, PII exposure. Then:
> 1. **Standards**: naming conventions, layer definitions, modeling guidelines, PR checklist, documented in the repo.
> 2. **Automated enforcement**: CI checks (`dbt_project_evaluator`, required tests and docs, contracts), so standards aren't just a wiki page.
> 3. **Ownership**: every model and source has an owner in `meta`, with CODEOWNERS.
> 4. **Classification and protection**: automatic classification + tag-based masking + RBAC templates.
> 5. **Quality SLAs**: freshness and test SLAs per tier (gold models stricter), monitored with alerts.
> 6. **Catalog and lineage**: dbt docs/Explorer or a catalog fed by dbt artifacts and Snowflake metadata.
> 7. **Federated operating model**: a governance council with domain reps. Platform provides tools, domains apply them.
> 8. Measure adoption: test coverage %, documented %, incidents, time-to-detect.

**🔴 Q5. How do you monitor pipeline health?** (JD bullet: monitor and troubleshoot)
> - **Orchestrator**: task/DAG failures, retries, duration vs SLA, alerts to Slack/PagerDuty.
> - **dbt artifacts** (`run_results.json`, `manifest.json`) loaded into Snowflake (e.g., Elementary or dbt_artifacts package): model run times, test failures over time.
> - **Freshness**: dbt source freshness, DMF `FRESHNESS`, `SYSTEM$PIPE_STATUS` for Snowpipe, `TASK_HISTORY`, dynamic table refresh history.
> - **Volume anomalies**: row-count trends per load.
> - **Cost**: credits per pipeline via query tags, resource monitor alerts.
> - **Dashboards + on-call runbooks**, with incident post-mortems feeding improvements.

---

## 18. Scenario / System Design Questions

> **Framework for any design question (say it out loud):**
> 1. **Clarify requirements**: sources, volume and velocity, latency SLA, consumers, history needs, PII/compliance, budget.
> 2. **High-level architecture**: ingest → land → transform → serve → govern → observe.
> 3. **Deep-dive** the hardest part (CDC, scale, late data, security).
> 4. **Trade-offs**: cost vs latency, simplicity vs flexibility.
> 5. **Operations**: monitoring, failure handling, CI/CD, ownership.

### S1. Design a near-real-time order analytics platform on Snowflake.
**Requirements to clarify:** ~50M orders/day from a Postgres OLTP plus clickstream from Kafka. Dashboards within 5 minutes. 3 years of history. PII present.

**Design**
```
Postgres ──(CDC: Debezium→Kafka / Fivetran / Openflow)──┐
Clickstream (Kafka) ────────────────────────────────────┼─► Snowpipe Streaming / Kafka connector
                                                         ▼
RAW (VARIANT + kafka metadata, append-only, transient where re-creatable)
   │  stream / dynamic tables (TARGET_LAG = '2 minutes')
   ▼
STAGING (typed, deduped by event_id/offset, latest-state CDC apply via MERGE)
   ▼
MARTS: fct_orders (incremental, clustered by order_date), dim_customer (SCD2 snapshot), agg tables for dashboards
   ▼
BI warehouse (multi-cluster)  │ secure views / shares for other domains
Governance: PII tag-based masking, RBAC per layer · Observability: freshness DMFs, alerts · CI/CD: dbt + GitHub Actions
```
**Talking points:** dedupe for at-least-once delivery; handle out-of-order CDC events by keeping the latest `lsn/updated_at` per key; split hot (minutes) from cold (daily batch) processing to control cost; cluster facts by date; separate warehouses for ingest, transform, and BI; dynamic tables vs streams+tasks trade-off; backfill path from the raw layer.

### S2. You're joining a team where dbt runs take 4 hours and Snowflake costs are rising 20% a month. What do you do in your first 30–60 days?
> 1. **Measure**: dbt artifacts → model timings. `QUERY_HISTORY`/`QUERY_ATTRIBUTION_HISTORY` by `query_tag` → top 20 models by time and credits. Warehouse utilization and idle time.
> 2. **Quick wins**: auto-suspend 60s, right-size warehouses, kill duplicate and unused models (no downstream exposures, no queries in `ACCESS_HISTORY`), transient staging, fix the obvious exploding joins and `SELECT *`.
> 3. **Structural**: convert big table rebuilds to incremental with `incremental_predicates`, cluster big facts, parallelize with threads and per-model warehouses, split the DAG into hourly-critical vs daily jobs, Slim CI so CI isn't rebuilding everything.
> 4. **Governance**: resource monitors and budgets, query tags, a cost dashboard per domain, statement timeouts.
> 5. **Report outcomes**: e.g., "runtime 4h → 70m, monthly credits −35%." Then make standards part of PR review to prevent regression.

### S3. Design a metadata-driven ingestion framework to onboard 200 source tables.
> Config table/YAML per source object (source, target, load type, keys, watermark, schedule, PII flag, owner, quality rules) → generic Snowflake procedures (or Python/Snowpark) for ingest, validate, and merge → Snowpipe/COPY for file sources, CDC tool for databases → audit and control tables → orchestration generated from config (Airflow dynamic DAGs or task graphs) → dbt staging models generated with `codegen`. Onboarding = PR adding config. Benefits: consistency, speed, centralized logging and quality, easy governance.

### S4. A federated domain team wants to publish a "Customer 360" data product for other domains. How do you design it?
> - Owner: Customer domain. Conformed `dim_customer` (SCD2) + `customer_360` wide table.
> - Built in their dbt project. Public, **contracted, versioned** models with documented grain, definitions, freshness SLA, and quality tests.
> - Exposed as **secure views** in a `PUBLISHED` schema, accessed through RO roles or an **internal listing**. Tag-based masking on PII so consumers without PII roles see masked values.
> - Identity resolution: deterministic matching on email/phone hash, survivorship rules, and a cross-reference table of source IDs → `customer_sk`.
> - Change management: versioning, deprecation notices, consumers tracked through exposures and `ACCESS_HISTORY`.

### S5. Design SCD2 history for a 500M-row customer table updated by daily CDC.
> Use dbt snapshots (timestamp strategy) or a custom MERGE with `hash_diff`. Process **only changed keys** from CDC (not a full compare). Cluster the dimension by `is_current`/`customer_id` ranges or `valid_to`, or keep a separate **current-state table** for fast lookups and the history table for as-of queries. Exclude volatile, non-analytic columns from the hash. Mini-dimension for fast-changing attributes. Backfill from raw CDC history if needed.

### S6. How would you set up a Snowflake account from scratch for a new data platform?
> - **Org/accounts**: prod and non-prod accounts (or one account with env databases), with replication for DR.
> - **Security**: SSO + SCIM, MFA, network policies, key-pair service users, ACCOUNTADMIN limited to 2–3 people.
> - **RBAC**: access roles per database/schema (RO/RW/ADMIN), functional roles, everything rolling up to SYSADMIN, managed access schemas. All of it via Terraform.
> - **Databases**: RAW, ANALYTICS (staging, intermediate, marts), SANDBOX, GOVERNANCE (policies, tags), UTIL/OPS (audit, logs).
> - **Warehouses** per workload with resource monitors and timeouts.
> - **Integrations**: storage integrations, notification integrations, external access, Git repository.
> - **Governance**: tag library, masking policies, classification, DMFs.
> - **Tooling**: dbt project template, CI/CD, orchestration, monitoring dashboards, cost reporting.

### S7. Snowflake vs Databricks (or Spark) for this workload?
> Snowflake is strongest for SQL-centric ELT, BI concurrency, governance, data sharing, and low admin. Spark/Databricks is strongest for heavy custom code, ML at scale, and open lake processing. Many orgs use both, with Iceberg as a shared table format. For this role's workloads (pipelines, modeling, governance), Snowflake + dbt is ideal. I'd use Snowpark or Spark only where SQL doesn't fit.

---

## 19. Troubleshooting Scenarios

| # | Symptom | Investigation | Likely causes & fixes |
|---|---|---|---|
| T1 | **Dashboard numbers doubled overnight** | Check row counts per day, uniqueness test on grain, recent deploys/PRs, source duplicates | Join fan-out from a new dimension with duplicate keys; source resent data; incremental model missing `unique_key` / lookback overlap. Fix the logic, dedupe, backfill the affected range, add a uniqueness test |
| T2 | **Pipeline that took 20 min now takes 2 hrs** | Query profile vs older runs (`QUERY_HISTORY` by tag), data volume growth, queueing, warehouse size changes | Volume spike, spilling, lost pruning (clustering degraded / filter changed), queueing from concurrency, a MERGE scanning the whole target. Fix with incremental predicates, scale up, separate warehouse |
| T3 | **Snowpipe stopped loading** | `SYSTEM$PIPE_STATUS`, `COPY_HISTORY`, event notification config, IAM | Paused pipe (ownership change), notification removed, file format change, IAM trust broken. Fix, then `ALTER PIPE … REFRESH` |
| T4 | **dbt incremental model missing records** | Compare source vs target for date range, check watermark logic | Strict `>` on `updated_at` + late arrivals or clock skew; timezone mismatches. Add a lookback, use `>=` with dedupe, align timestamps (`TIMESTAMP_NTZ` vs `LTZ`) |
| T5 | **MERGE fails: "Duplicate row detected during DML action"** | Check source keys: `GROUP BY key HAVING COUNT(*) > 1` | Source batch has multiple rows per key. Dedupe with `QUALIFY ROW_NUMBER()` by latest timestamp |
| T6 | **Stream is stale** | `SHOW STREAMS` → `STALE`, `STALE_AFTER` | Not consumed within retention. Recreate and reconcile the gap. Add monitoring |
| T7 | **Task not running** | `SHOW TASKS` state, `TASK_HISTORY` errors | Task suspended (created suspended, or auto-suspended after failures), owner lacks `EXECUTE TASK` privilege or warehouse usage, `WHEN` condition false, predecessor failed |
| T8 | **Query fails with "Insufficient privileges"** | `SHOW GRANTS TO ROLE`, current role/secondary roles, future grants | Object recreated (`CREATE OR REPLACE`) lost grants; missing USAGE on database/schema/warehouse. Use `copy_grants`/dbt `grants`, future grants |
| T9 | **Credits spiked this week** | `WAREHOUSE_METERING_HISTORY`, serverless histories, `QUERY_ATTRIBUTION_HISTORY` | Warehouse left running (auto-suspend off), a new clustering key churning, runaway ad-hoc query, a loop of small queries, dbt full refresh. Fix, then add monitors and timeouts |
| T10 | **Storage cost high on a "small" table** | `TABLE_STORAGE_METRICS` | Time Travel/Fail-safe bytes from daily full reloads. Make it transient, reduce retention, go incremental |
| T11 | **Timestamps off by hours** | Data types and session `TIMEZONE` | Mix of `TIMESTAMP_NTZ/LTZ/TZ`. Standardize on UTC `TIMESTAMP_NTZ` (or `TZ`) in pipelines, convert at presentation |
| T12 | **Query slower after resizing the warehouse** | Cold cache, query profile | Warehouse cache was cleared by resize/suspend. The query isn't parallelizable (small data, single-threaded step), or queueing. Size isn't always the answer |

**How to narrate troubleshooting in interviews:** *Detect* (alert or user) → *Contain* (pause downstream, communicate) → *Diagnose* (logs, profile, history, diff vs last good) → *Fix* (code + data backfill, using Time Travel if needed) → *Prevent* (test, monitor, runbook, post-mortem).

---

## 20. Behavioral Questions

> Use **STAR** (Situation, Task, Action, Result) with **numbers**. For senior roles, emphasize **ownership, influence without authority, mentoring, trade-off decisions, and cross-team collaboration** (the JD stresses federated teams and communication). Prepare **6–8 stories** that you can reuse across questions.

**Story bank to prepare (map to JD bullets)**

| Story | JD alignment | Likely questions it answers |
|---|---|---|
| Built/re-architected a Snowflake pipeline end to end | Develop & optimize Snowflake solutions; pipelines/ETL | "Proudest project", "Biggest technical challenge" |
| Performance/cost optimization with measurable results | Optimize; performance tuning | "Tell me about improving something", "Data-driven decision" |
| Production incident you led to resolution | Monitor & troubleshoot | "Handling pressure", "Failure", "On-call" |
| Introduced data quality/governance standards across teams | Governance & best practices | "Influence without authority", "Driving change" |
| Collaboration with a domain/federated team with conflicting needs | Collaborate with Federated teams | "Conflict", "Stakeholder management" |
| Built reusable framework/stored procedures used by others | Reusable, modular procedures | "Scalability", "Leverage" |
| Implemented CI/CD/dbt adoption | Git + CI/CD | "Improving team process" |
| Mentored a junior engineer / led code reviews | Senior expectations | "Leadership", "Mentoring" |
| A mistake you made and what you changed | Integrity | "Failure", "Feedback" |

**Common questions (with what interviewers want to hear)**
1. **Tell me about yourself.** 90 seconds: current role, years and scope, Snowflake/dbt highlights with one metric, why this role.
2. **Most complex pipeline you've built?** Scale, constraints, design decisions, trade-offs, results.
3. **A time you disagreed with a stakeholder or another team.** Listen, use data, find shared goals, disagree-and-commit, the relationship stays intact.
4. **A production failure you caused or handled.** Ownership, calm communication, fix, *systemic prevention*.
5. **How do you prioritize competing requests from multiple domain teams?** Business impact, SLAs, effort, transparency (backlog, roadmap), saying no with alternatives.
6. **How did you introduce a standard that people resisted?** Start with pain points, pilot, automate enforcement, make the right way the easy way, show wins.
7. **How do you mentor others / do code reviews?** Constructive, teach the why, checklists, pair on hard problems, grow ownership.
8. **Tell me about a time you had to learn something quickly.** e.g., adopting dynamic tables or Snowpark. How you learned and delivered.
9. **How do you explain technical issues to non-technical stakeholders?** Impact first, plain language, options with trade-offs, timeline.
10. **A decision you made with incomplete information.** Assumptions stated, reversible choices, validation checkpoints.
11. **Why are you leaving / why this company?** Positive, aligned with the role (Snowflake depth, platform/governance scope, federated model).
12. **Where do you want to be in 3 years?** Growth in architecture/technical leadership aligned with the team.

**Senior-level signals to weave in:** "I set the standard…", "I wrote the design doc and got buy-in from…", "I made the trade-off because…", "I automated the guardrail so…", "I mentored…", "the measurable result was…".

---

## 21. Questions to Ask the Interviewer

**About the platform**
1. What does "federated teams" look like in practice: data mesh, domain-owned dbt projects, or a central team serving domains?
2. What's the current stack around Snowflake: ingestion (Fivetran/Informatica/custom), orchestration (Airflow/dbt Cloud/Tasks), and is there an ETL modernization (e.g., Informatica → dbt) underway?
3. How is Snowflake infrastructure managed: Terraform/schemachange, or manual?
4. What does CI/CD look like today for pipelines, and what would you like improved?

**About the role and challenges**
5. What are the biggest pain points right now: reliability, cost, performance, data quality, or governance?
6. What would success look like for this role at 90 days and at one year?
7. How mature is data governance today (catalog, lineage, PII classification, quality monitoring)?
8. How is on-call/incident management handled for data pipelines?

**About the team**
9. How large is the data engineering team, and how is work split between platform and domain engineers?
10. How are technical decisions made and documented (design docs, architecture reviews)?

---

## 22. Last-Night Cheat Sheet

**Snowflake numbers & facts**
- 3 layers: storage · compute (virtual warehouses) · cloud services (billed only beyond 10% of daily compute)
- Micro-partitions: **50–500 MB uncompressed**, columnar, immutable, min/max metadata → pruning
- Warehouse credits: XS 1 → S 2 → M 4 → L 8 → XL 16 → 2XL 32 → 3XL 64 → 4XL 128 (doubling). Per-second billing, **60s minimum**
- Multi-cluster (Enterprise): Standard vs Economy scaling. Scale **up** = complex queries/spill. Scale **out** = concurrency
- Time Travel: Standard 1 day. Enterprise up to **90 days** (permanent). Transient/temporary 0–1 day
- Fail-safe: **7 days**, permanent only, Snowflake Support only
- Result cache **24h** (renewed on reuse, up to **31 days**). Warehouse cache lost on suspend. Metadata cache for COUNT/MIN/MAX
- Load metadata: COPY **64 days**, Snowpipe **14 days**. Ideal file size **100–250 MB compressed**
- COPY `ON_ERROR` default `ABORT_STATEMENT`. Snowpipe default `SKIP_FILE`
- Only `NOT NULL` enforced (standard tables). PK/FK informational (`RELY` for join elimination)
- Stream offset advances only on **committed DML** consumption. Staleness ↔ retention (extension up to `MAX_DATA_EXTENSION_TIME_IN_DAYS`, default 14)
- Tasks are created **suspended**. Resume children before root. `WHEN SYSTEM$STREAM_HAS_DATA` skips runs without compute
- Dynamic tables: `TARGET_LAG`, `DOWNSTREAM`, incremental vs full refresh
- MVs: single table, no joins, Enterprise, serverless maintenance
- Enterprise-only highlights: multi-cluster, 90-day Time Travel, MVs, search optimization, QAS, masking & row access policies, tags, ACCESS_HISTORY
- Business Critical: Tri-Secret Secure, PrivateLink, failover/failback, HIPAA/PCI
- ACCOUNT_USAGE: 1-year retention, up to ~45 min–3 hr latency. INFORMATION_SCHEMA: real-time, short retention
- Resource monitors = warehouses only. Budgets for serverless/overall spend
- Policies: use `IS_ROLE_IN_SESSION()`. Tag-based masking scales
- Cloning: zero-copy, instant. Internal named stages not cloned. Tasks/pipes cloned suspended/paused. Grants not copied (except `COPY GRANTS`)

**Performance checklist:** pruning (partitions scanned/total) → spilling (local/remote) → exploding joins → skew → queueing → caches → rewrite (columns, filters, pre-aggregate, `QUALIFY`, `UNION ALL`, `NOT EXISTS`) → clustering / search optimization / MV / dynamic table → right-size warehouse

**Modeling checklist:** business process → **grain** → dimensions → facts. Star > snowflake. Conformed dims. Surrogate (hash) keys. Unknown members. SCD 0/1/2/3/4/6. Fact types: transaction, periodic snapshot, accumulating snapshot, factless. Additive/semi/non-additive. Junk, degenerate, role-playing, mini, bridge

**dbt checklist:** `ref`/`source` · materializations (view, table, incremental, ephemeral, snapshot, dynamic_table) · incremental strategies (merge, append, delete+insert, insert_overwrite, microbatch) · `is_incremental()`, `{{ this }}`, lookback, `incremental_predicates`, `on_schema_change` · snapshots (timestamp vs check, hard_deletes) · tests (unique, not_null, accepted_values, relationships, singular, unit tests, freshness, severity, store_failures) · `build` vs `run` · selectors (`+`, `@`, `tag:`, `state:modified+`) · Slim CI with `--defer --state` · `generate_schema_name` · contracts, versions, groups/access, Mesh · Snowflake tables transient by default

**Git/CI/CD checklist:** trunk-based + PRs + protected main + CODEOWNERS · merge vs rebase vs squash · reset vs revert · cherry-pick hotfix · secrets never in Git (rotate if leaked) · CI = lint (SQLFluff) → compile → Slim CI build/test in PR schema → standards/contract checks → cleanup · CD = migrations (schemachange/Terraform/native Git) → dbt prod → artifacts upload · WAP + blue/green with clone & `SWAP` · rollback via `git revert` + Time Travel · key-pair auth for service users

---

## 23. 7-Day Study Plan

| Day | Focus | Do this |
|---|---|---|
| **1** | Snowflake architecture, warehouses, storage, Time Travel, cloning | Read sections 1–3. Explain the architecture out loud in 2 minutes. In a trial account: create warehouses, clone a table, recover with Time Travel |
| **2** | Loading, Snowpipe, streams, tasks, dynamic tables, semi-structured | Sections 4–6. Build: stage → COPY → stream → task MERGE. Flatten a nested JSON |
| **3** | Procedures, security, governance, cost | Sections 7–9. Write a parameterized Snowflake Scripting procedure. Create a masking policy and row access policy and test them with two roles |
| **4** | Performance tuning | Section 10. Run queries on `SNOWFLAKE_SAMPLE_DATA` (TPCH_SF100), read Query Profiles, trigger spilling on an XS warehouse, compare pruning with/without good filters |
| **5** | Data warehousing + dimensional modeling + ELT design | Sections 11–13. Whiteboard a star schema for an e-commerce business with SCD2 customer and an accumulating snapshot fact. Practice explaining the grain |
| **6** | dbt + Git + CI/CD + data quality | Sections 14–17. Build a small dbt project on Snowflake (sources, staging, incremental fact, snapshot, tests). Set up a GitHub Actions CI with `dbt build` |
| **7** | Scenarios, troubleshooting, behavioral, mock interview | Sections 18–22. Answer S1–S7 out loud, timed. Rehearse 6–8 STAR stories with metrics. Prepare questions. Review the cheat sheet |

**Final tips**
- Lead with the short answer, then go deeper only as far as the interviewer wants.
- Every claim of experience should come with a **number** (rows, TB, minutes saved, % cost reduced, SLA).
- When unsure, reason out loud: "I haven't used X directly, but here's how I'd approach it given how Snowflake handles Y…"
- For design questions, **always clarify requirements first** and **state trade-offs**. That's what separates senior candidates.

**Good luck! 🚀**
