# RBO tests with analyzed benchmark data

`helpers/kqp_analyzed_benchmarks.h` exposes `LoadTpchAndAnalyze(double scale, bool columnStore = false)`
and `LoadTpcdsAndAnalyze(double scale, bool columnStore = false)`. Both return an owning
`TAnalyzedBenchmark` with a reusable query session, actor runtime, table paths,
and row counts read from the analyzed table summaries. Keep the returned object
alive while using its session.

The helpers create a local database with statistics enabled, initialize row or column
schemas with decimal columns through `ydb workload`, generate the requested
scale with one uploader, and execute `ANALYZE` on every table.

`data/<benchmark>_sf0.1_<row|column>.json` contains base row counts and byte sizes
collected once from the scale-0.1 datasets. The four snapshots were captured from
the statistics service after checking counts against the analyzed table summaries.
The helpers pass only their `tables` objects to `OptOverrideStatistics`, replacing
the variable SchemeShard row counts and sizes. All column statistics, including
CMS, NDV and histograms, still come from each run's `ANALYZE`.

Every file records its scale, workload and storage type. The helpers validate this
metadata **before starting the server**, reject any scale other than 0.1, and check
captured row counts against the newly analyzed table summaries. The snapshots
allow only `n_rows` and `byte_size`; column overrides are rejected. Normal tests
never rewrite these snapshots. There is no forced compaction or statistics-settling
wait in these tests.

`KqpRboStatistics::RejectStatisticsScaleMismatch` verifies that another scale
cannot accidentally use these snapshots. Supporting another scale requires its
own collected snapshot and an explicit update to snapshot selection and validation.

`KqpRboStatistics::ExplainTpchFilterAfterAnalyze` uses TPCH scale 0.1. It checks
actual customer counts, probes the computed `c_mktsegment` count-min sketch for
`HOUSEHOLD`, and checks the scan and filter cardinalities in Explain JSON.
The complete plan is saved as `testing_out_stuff/tpch_filter.explain.json`.

## Join-order regression tests

The separate `statistics/join_order` target contains
TPCH and TPCDS twins in `KqpJoinOrderAnalyzedStatistics`, for both row and column
tables. They load and analyze each
benchmark once and compile the benchmark queries with `EExecMode::Explain`.
They do not execute the benchmark queries. TPCH covers all 22 queries; TPCDS
covers 97 queries, retaining the original suite's exclusions for queries 17 and 44.

The original `rbo/join_order` tests are retained with their existing statistics
fixtures and baselines. Shared SQL uses relative table names; `ReadBenchmarkQuery`
sets `PRAGMA TablePathPrefix` to the original schema path by default, or to the
analyzed dataset path supplied by the new tests. These new tests have their own `join_order/canondata/` baselines.
Canonical JSON records join trees, join algorithms, tables, and shuffles using
`GetDetailedJoinOrder`, as in the original suite. Complete Explain JSON files are
also saved under `testing_out_stuff/<benchmark>_sf<scale>_<row|column>/` for inspection.

Parameters:

- `JOIN_ORDER_SCALE`: defaults to `0.1`; other values are rejected by the captured
  statistics guard. Baselines are separated by scale and storage type.
- `JOIN_ORDER_QUERIES`: comma-separated query IDs; explicit selection also allows
  running the otherwise excluded queries.
- `CANONIZE_JOIN_ORDER_TESTS=true`: write baselines for successfully explained
  queries. Normal runs compare against existing baselines and fail if one is missing.

Generate or update baselines explicitly:

```bash
./ya make --build relwithdebinfo -tA ydb/core/kqp/ut/rbo/statistics/join_order \
    --test-param CANONIZE_JOIN_ORDER_TESTS=true 2>&1 | tail -n 40
```

Run the filter test and compare both benchmarks with their baselines:

```bash
./ya make --build relwithdebinfo -tA ydb/core/kqp/ut/rbo/statistics 2>&1 | tail -n 40
```

Update one selected baseline:

```bash
./ya make --build relwithdebinfo -tA ydb/core/kqp/ut/rbo/statistics/join_order \
    -F 'KqpJoinOrderAnalyzedStatistics::TPCH*' \
    --test-param JOIN_ORDER_QUERIES=2 \
    --test-param CANONIZE_JOIN_ORDER_TESTS=true 2>&1 | tail -n 40
```

Review generated baseline changes and rerun without canonization to verify them.

## Correctness tests

The `statistics/correctness` target runs TPCH and TPCDS at scale 0.1 with both
row and column tables in `KqpCorrectnessAnalyzedStatistics`. It uses the same
data generation, decimal schemas, base-statistics snapshots and per-table
`ANALYZE` as the join-order tests. TPCH covers all 22 queries; TPCDS retains the
join-order suite's exclusions for queries 17 and 44.

Each test loads and analyzes the dataset once. For each selected query, it
disables NewRBO, executes the old optimizer, enables NewRBO, and executes again,
with fallback disabled throughout. Configuration updates are acknowledged by
every compiler and invalidate cached plans before execution. It immediately compares
result-set counts, column names and types, and values against the old
optimizer's results. Both results are released before the next query.
Truncated results fail the test. No canonical data is read or written.

Run all four benchmark/storage combinations:

```bash
./ya make --build relwithdebinfo -tA ydb/core/kqp/ut/rbo/statistics/correctness 2>&1 | tail -n 60
```

Use `CORRECTNESS_QUERIES` to select comma-separated query IDs, including the
otherwise excluded queries when investigating them:

```bash
./ya make --build relwithdebinfo -tA ydb/core/kqp/ut/rbo/statistics/correctness \
    -F 'KqpCorrectnessAnalyzedStatistics::TPCH*' \
    --test-param CORRECTNESS_QUERIES=2,5 2>&1 | tail -n 60
```
