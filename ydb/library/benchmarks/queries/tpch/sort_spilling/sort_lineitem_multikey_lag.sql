-- Sort Spilling Test: Sort lineitem by multiple keys, compute LAG on price
-- Uses date filter to reduce dataset. Multi-key sort (ASC/DESC mix) with LAG.
-- LAG only needs to keep 1 previous row, so memory usage is bounded.

$filtered = (
select
    l_orderkey,
    l_returnflag,
    l_shipdate,
    l_extendedprice
from
    `column/tpch/s10000/lineitem`
where
    l_shipdate >= Date('1996-01-01')
    and l_shipdate < Date('1997-01-01')
);

$with_lag = (
select
    l_orderkey,
    l_returnflag,
    l_shipdate,
    l_extendedprice,
    lag(l_extendedprice) over w as prev_price
from $filtered
window w as (order by l_returnflag asc, l_shipdate desc, l_orderkey asc)
);

-- Aggregate price differences by returnflag
select
    l_returnflag,
    count(*) as cnt,
    sum(l_extendedprice - coalesce(prev_price, l_extendedprice)) as total_price_diff,
    avg(l_extendedprice - coalesce(prev_price, l_extendedprice)) as avg_price_diff,
    min(l_extendedprice - coalesce(prev_price, l_extendedprice)) as min_price_diff,
    max(l_extendedprice - coalesce(prev_price, l_extendedprice)) as max_price_diff
from $with_lag
group by l_returnflag
order by l_returnflag;
