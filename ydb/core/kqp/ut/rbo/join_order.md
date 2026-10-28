# RBO canonical join orders

`kqp_join_order_ut.cpp` has two benchmark tests: `TPCH` (22 YQL queries) and
`TPCDS` (99 YQL queries). They use the queries and column-store schemas already
used by `KqpRboYql::TPCH_YQL` and `KqpRboYql::TPCDS_YQL`. They explain queries
with the new optimizer, fallback disabled, and CBO level 4. Snapshots record
join trees, algorithms, and shuffles using `GetDetailedJoinOrder`. All queries,
schemas, statistics, and snapshots are loaded from the RBO-local `data/` directory.

`RunJoinOrderBenchmark` runs a benchmark and reports all query failures together.
Each benchmark passes a disabled query-ID set for missing snapshots and a
separate `mismatched_join_orders` set for known canonical mismatches. Explicit
`JOIN_ORDER_QUERIES` selection overrides both sets, so excluded queries can be
retried or recanonicalized without editing the test. Currently TPC-H queries
1, 6, 12, 14, 19, and 22 are enabled. All TPC-DS queries are skipped: 95 lack
snapshots and the remaining four have known mismatches.

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
