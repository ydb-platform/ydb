# TPC-H Sort Spilling Test Queries

These queries are designed to test sort spilling on TPC-H scale 10000 data.
They produce large intermediate result sets that require sorting, which should
trigger spilling when memory limits are configured appropriately.

All queries are designed so that the sort cannot be optimized away or replaced
with a TopSort (ORDER BY ... LIMIT). After sorting, the queries perform
additional computation that depends on the full sorted order (window functions,
running aggregates, etc.).

Table paths use the format `column/tpch/s10000/<table>`.

## Usage

Run on a cluster with TPC-H 10000 data loaded in column tables.

Example:
```bash
ydb -e grpc://cluster:2135 -d /Root/db scripting yql -f sort_lineitem_running_sum.sql
```

## Queries

1. **sort_lineitem_running_sum.sql** - Sort lineitem by shipdate, compute running sum of quantity.
   Uses window function over sorted data. ~600M rows at scale 10000.

2. **sort_lineitem_row_number.sql** - Sort lineitem by extendedprice DESC, assign row numbers.
   Uses ROW_NUMBER() window function. ~600M rows.

3. **sort_lineitem_multikey_lag.sql** - Sort lineitem by multiple keys, compute LAG on price.
   Tests multi-key sort with window function.

4. **sort_orders_running_total.sql** - Sort orders by orderdate, compute running total of price.
   ~150M rows at scale 10000.

5. **sort_orders_rank.sql** - Sort orders by totalprice DESC, compute RANK.
   Tests ranking over large dataset.

6. **sort_join_lineitem_orders_window.sql** - Join lineitem with orders, sort, compute window agg.
   Tests sort spilling after a join producing a very large result.

7. **sort_aggregated_lineitem_ntile.sql** - Aggregate lineitem by orderkey, sort by revenue,
   compute NTILE(100). Tests sort after aggregation.

8. **sort_lineitem_by_comment_lead.sql** - Sort by string column (l_comment), compute LEAD.
   Tests spilling with variable-length string sort keys.

9. **sort_partsupp_running_avg.sql** - Sort partsupp by multiple keys, compute running avg.
   ~80M rows at scale 10000.

10. **sort_lineitem_dense_rank.sql** - Sort lineitem by returnflag+shipdate, compute DENSE_RANK.
    Tests dense ranking over large dataset.
