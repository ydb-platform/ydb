# RBO canonical join orders

`kqp_join_order_ut.cpp` has two benchmark tests: `TPCH` (22 YQL queries) and
`TPCDS` (99 YQL queries). They use the queries and column-store schemas already
used by `KqpRboYql::TPCH_YQL` and `KqpRboYql::TPCDS_YQL`. They explain queries
with the new optimizer, fallback disabled, and CBO level 4. Snapshots record
join trees, algorithms, and shuffles using `GetDetailedJoinOrder`. All queries,
schemas, statistics, and snapshots are loaded from the RBO-local `data/` directory.

`RunJoinOrderBenchmark` runs a benchmark and reports all query failures together.
Each benchmark passes a disabled query-ID set. Explicit `JOIN_ORDER_QUERIES`
selection overrides that set, so excluded queries can be run or canonized without
editing the test. All 22 TPC-H queries and 97 TPC-DS queries are enabled. Only
TPC-DS queries 17 and 44 are skipped, matching `KqpRboYql::TPCDS_YQL`.

The suite builds in the separate target `ydb/core/kqp/ut/rbo/join_order` to
avoid the main RBO binary's debug-section relocation limit. Its source remains
in `kqp_join_order_ut.cpp`.

## Statistics

`JOIN_ORDER_STATISTICS` accepts `normal` (default) or `small`.
Statistics are loaded unchanged from `data/stats/`:

| Benchmark | Normal | Small |
| --- | --- | --- |
| TPCH | tpch1000s.json | tpch100s.json |
| TPCDS | tpcds1000s.json | tpcds10s.json |

`tpcds10s.json` is not present yet. Add it before running the TPC-DS small
profile. The partial `tpcds10s_39_query.json` is not used. A missing statistics
file fails with its full path; statistics are never scaled.

Snapshots use the copied flat filenames in `data/join_order/`:
`tpch<ID>_1000s_column_store.json` and `tpcds<ID>_1000s_column_store.json`
for normal statistics; the small profile uses `100s` and `10s`, respectively.
Comparison ignores JSON formatting but preserves tree and child order.
Missing snapshots fail; generate any missing baselines before comparing those queries.

## Commands

Run all enabled queries against normal statistics:

```bash
./ya make --build relwithdebinfo -tA ydb/core/kqp/ut/rbo/join_order -F 'KqpJoinOrderNewRbo::*' 2>&1 | tail -n 30
```

Run selected TPC-H queries against small statistics:

```bash
./ya make --build relwithdebinfo -tA ydb/core/kqp/ut/rbo/join_order -F 'KqpJoinOrderNewRbo::TPCH' --test-param JOIN_ORDER_QUERIES=2,5 --test-param JOIN_ORDER_STATISTICS=small 2>&1 | tail -n 30
```

Update one snapshot:

```bash
./ya make --build relwithdebinfo -tA ydb/core/kqp/ut/rbo/join_order -F 'KqpJoinOrderNewRbo::TPCH' --test-param JOIN_ORDER_QUERIES=2 --test-param JOIN_ORDER_STATISTICS=normal --test-param CANONIZE_JOIN_ORDER_TESTS=true 2>&1 | tail -n 30
```

Update all enabled queries for both benchmarks using normal statistics:

```bash
./ya make --build relwithdebinfo -tA ydb/core/kqp/ut/rbo/join_order -F 'KqpJoinOrderNewRbo::*' --test-param CANONIZE_JOIN_ORDER_TESTS=true 2>&1 | tail -n 30
```

Use the benchmark filter to update just one benchmark; add
`JOIN_ORDER_STATISTICS=small` to update the small profile after providing its
statistics file. Canonicalization writes directly to this checkout and updates only
queries with successful explain plans. A failed query leaves its snapshot
untouched and fails the benchmark. Review the snapshot diff, then rerun without
canonicalization. The original optimizer's snapshots are never overwritten.
