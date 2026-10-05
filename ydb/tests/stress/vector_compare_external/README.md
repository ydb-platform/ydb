# Compare YDB and PostgreSQL on exported ANN datasets

Workflow: `.github/workflows/compare_vector_databases.yml`.

Every manual run downloads one dataset from S3, verifies the exports, loads both
remote databases, builds indexes, warms up, and measures throughput. It does not
start or upgrade database servers. The YDB CLI is built from the selected GitHub
ref, which must support the selected index type on the target YDB server.

## Connections and runner

Use an auto-provisioned Linux `build-preset-release` runner with network access to
both databases and S3. It needs the normal YDB build toolchain, Python 3 with
`venv`, and enough local disk for both compressed exports plus the CLI build.
The workflow installs `boto3` in a temporary virtual environment. If `psql` or
`pgbench` is missing, it uses `sudo apt-get` to install PostgreSQL client tools and the package providing pgbench.
PostgreSQL must provide the pgvector extension (0.7+ for these sparse exports).
The database users need permission to create tables/indexes; the PostgreSQL user
also needs CREATE on the database and permission to install pgvector if absent.

Configure GitHub Actions secrets:

- `PG_DSN`: PostgreSQL connection string, e.g.
  `postgresql://user:password@postgres-host:5432/benchmark?sslmode=require`.
  `pg_dsn_secret` selects a different secret name when needed. The password is
  not entered as a plaintext workflow input. libpq parses the DSN into child
  process environment variables; it is not passed in command-line arguments.
- `VECTOR_BENCH_YDB_TOKEN`: optional YDB authentication token.
- `VECTOR_S3_ACCESS_KEY_ID`, `VECTOR_S3_SECRET_ACCESS_KEY`: private S3 credentials.
  `VECTOR_S3_SESSION_TOKEN` is optional. Public buckets can use `s3_unsigned=true`.

Pass `ydb_endpoint` and `ydb_database` as workflow inputs. Connections must point
to existing databases reserved for benchmarking.

## S3 layout

The default endpoint is `https://storage.yandexcloud.net`. Supply `s3_bucket` and
an optional common `s3_prefix`. Under that prefix, preserve the existing export
layout:

```text
ydb_vector_data/<dataset>/manifest.json
ydb_vector_data/<dataset>/base/part-*.parquet
ydb_vector_data/<dataset>/queries/part-*.parquet
pgbench_data/<dataset>/manifest.json
pgbench_data/<dataset>/base.copy
pgbench_data/<dataset>/queries.copy
```

For example, upload the prepared local exports with:

```bash
aws --endpoint-url https://storage.yandexcloud.net s3 sync \
  /home/smurylev/benchmark/ydb_vector_data s3://YOUR_BUCKET/ann/ydb_vector_data \
  --exclude '*' --include '*/manifest.json' --include '*.parquet'
aws --endpoint-url https://storage.yandexcloud.net s3 sync \
  /home/smurylev/benchmark/pgbench_data s3://YOUR_BUCKET/ann/pgbench_data \
  --exclude '*' --include '*/manifest.json' --include '*.copy'
```

Then use `s3_prefix=ann`. Uploaded shell and SQL scripts are not executed. The
runner constructs commands using the requested dataset and fresh resource names.
It checks matching row counts, dimensions, metric and scaling across manifests,
then verifies every downloaded file's size and SHA-256 before touching databases.

| Dataset | Dimensions | Metric | PostgreSQL representation |
|---|---:|---|---|
| `text2image-10M` | 200 | inner product | `vector(200)` |
| `yfcc-10M` | 192 | Euclidean | `vector(192)` |
| `sparse` | 30109 | inner product | `sparsevec(30109)` |

YDB sparse exports are **dense float32 vectors**, about **1.06 TB of logical base
vector data** for the full dataset, even though Parquet compresses well. Imports
use 128-row batches for sparse, 2000 for the dense datasets. Provision server
storage and graph memory for the logical data size, not just the S3 objects.

## Benchmark settings

Select `dataset` and YDB `ydb_index=hnsw` or `vector_kmeans_tree`. PostgreSQL uses
HNSW with `m=16`, `ef_construction=200`. YDB HNSW uses the corresponding settings,
`min_rows=1`, and `delta_rows=10000`. YDB construction defaults to `levels=1`,
`clusters=10`; these are not physical partition counts. K-means search uses the
CLI's default one cluster per level. HNSW search breadth defaults to 50 on both
backends and can be changed with `ef_search`.

Defaults: 50 concurrent clients, 100-second measurements, 60-second warmups,
three iterations, top-10, first 1000 query IDs (capped to the available count).
Queries come from the exported query tables, not generated vectors. YDB cycles
through these queries; pgbench randomly samples the same ID range with replacement.
YDB loads query vectors before timing. PostgreSQL TPS includes the query-vector
primary-key lookup in each statement. Thus this measures the two existing tool
paths, rather than identical client-side execution.

Backends run sequentially, with order alternating between iterations. Download,
loading, index construction, and warmup are excluded from measured QPS. Failed
commands, zero successful queries, or any reported query errors fail the run.
A PostgreSQL EXPLAIN check requires the HNSW index in the search plan. Recall is
not measured; matching `ef_search` does not establish matching recall.

## Results and lifecycle

The job summary contains per-iteration and median QPS and the YDB/PostgreSQL
median ratio. Artifacts contain `report.md`, `results.json`, and sanitized
command logs, verified manifests, index descriptions and the PostgreSQL plan. Source data,
DSNs, and tokens are not uploaded as artifacts.

Each invocation uses a random `ann_<dataset>_<suffix>` resource prefix. Tables
from previous runs are never dropped. By default, the runner removes only its
own tables/schema on completion or failure. Set `keep_data=true` to retain them;
resource names are recorded in the report. Cleanup is best-effort after job
cancellation or runner failure; use the recorded prefix to remove leftovers.
Downloaded data is stored under `RUNNER_TEMP` and removed after the run. Index
construction can take many hours; the workflow has a 24-hour job limit.

Run orchestration tests without accessing remote services:

```bash
python3 -m unittest discover -s ydb/tests/stress/vector_compare_external -v
```

PostgreSQL output parsing follows the
[pgbench summary format](https://www.postgresql.org/docs/current/pgbench.html).
Connection strings are parsed using
[libpq](https://www.postgresql.org/docs/current/libpq-connect.html).
