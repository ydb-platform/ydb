-- Sort Spilling Test: Sort lineitem by multiple keys, compute LAG on price
-- TPC-H scale 10000: ~600M rows in lineitem
-- Multi-key sort (ASC/DESC mix) with LAG window function.

$with_lag = (
select
    l_orderkey,
    l_returnflag,
    l_shipdate,
    l_extendedprice,
    lag(l_extendedprice) over w as prev_price,
    l_extendedprice - coalesce(lag(l_extendedprice) over w, l_extendedprice) as price_diff
from
    `column/tpch/s10000/lineitem`
window w as (order by l_returnflag asc, l_shipdate desc, l_orderkey asc)
);

-- Aggregate price differences by returnflag
select
    l_returnflag,
    count(*) as cnt,
    sum(price_diff) as total_price_diff,
    avg(price_diff) as avg_price_diff,
    min(price_diff) as min_price_diff,
    max(price_diff) as max_price_diff
from $with_lag
group by l_returnflag
order by l_returnflag;
