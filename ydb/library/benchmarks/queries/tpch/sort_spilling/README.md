# TPC-H Sort Spilling Test Queries

These queries are designed to test sort spilling on TPC-H scale 10000 data
stored in column tables at `column/tpch/s10000/`.

All queries are designed so that the sort cannot be optimized away or replaced
with a TopSort (ORDER BY ... LIMIT). After sorting, the queries perform
additional computation that depends on the full sorted order (window functions
like ROW_NUMBER, LAG, LEAD, RANK, DENSE_RANK, NTILE). The results are then
sampled or aggregated to produce a small output that fits in memory.

Some queries use date or status filters to reduce the working set to a
manageable size (~50M-200M rows instead of the full 600M lineitem).

## Usage

Run on a cluster with TPC-H 10000 data loaded in column tables.

```bash
ydb -e grpc://cluster:2135 -d /Root/db \
  workload query run \
  -q "$(cat sort_lineitem_running_sum.sql)"
```

## Queries

1. **sort_lineitem_running_sum.sql** - Sort lineitem (1 year) by shipdate, ROW_NUMBER + sample.
   ~85M rows.

2. **sort_lineitem_row_number.sql** - Sort lineitem (returnflag='R') by extendedprice DESC,
   ROW_NUMBER + percentile sampling. ~200M rows.

3. **sort_lineitem_multikey_lag.sql** - Sort lineitem (1 year) by multiple keys (ASC/DESC mix),
   LAG on price, aggregate by returnflag. ~85M rows.

4. **sort_orders_running_total.sql** - Sort all orders by orderdate, ROW_NUMBER + sample.
   ~150M rows.

5. **sort_orders_rank.sql** - Sort orders (status='F') by totalprice DESC, RANK + sample.
   ~50M rows.

6. **sort_join_lineitem_orders_window.sql** - Join lineitem with orders (1 year filter),
   sort by totalprice+shipdate, ROW_NUMBER + sample. ~85M rows.

7. **sort_aggregated_lineitem_ntile.sql** - Aggregate lineitem by orderkey, sort by revenue,
   NTILE(100), aggregate per percentile. ~150M groups.

8. **sort_lineitem_by_comment_lead.sql** - Sort lineitem (1 year) by l_comment string,
   LEAD on price/comment, aggregate. Tests string sort keys. ~85M rows.

9. **sort_partsupp_running_avg.sql** - Sort all partsupp by 3 keys, ROW_NUMBER + sample.
   ~80M rows.

10. **sort_lineitem_dense_rank.sql** - Sort lineitem (1 year) by returnflag+shipdate,
    DENSE_RANK, aggregate by rank. ~85M rows.
