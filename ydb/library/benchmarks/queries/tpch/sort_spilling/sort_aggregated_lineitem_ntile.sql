-- Sort Spilling Test: Aggregate lineitem by orderkey, sort by revenue, compute NTILE
-- TPC-H scale 10000: ~150M groups (one per order)
-- Tests sort spilling after aggregation.

$agg = (
select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    sum(l_quantity) as total_quantity,
    count(*) as lineitem_count
from
    `{path}lineitem`
group by
    l_orderkey
);

$with_ntile = (
select
    l_orderkey,
    revenue,
    total_quantity,
    lineitem_count,
    ntile(100) over (order by revenue desc) as revenue_percentile
from $agg
);

-- Get statistics per percentile bucket
select
    revenue_percentile,
    count(*) as order_count,
    min(revenue) as min_revenue,
    max(revenue) as max_revenue,
    avg(revenue) as avg_revenue,
    sum(total_quantity) as total_qty
from $with_ntile
group by revenue_percentile
order by revenue_percentile;
