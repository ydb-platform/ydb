-- Sort Spilling Test: Sort lineitem by returnflag+shipdate, compute DENSE_RANK
-- TPC-H scale 10000: ~600M rows in lineitem
-- Tests dense ranking over large dataset with composite sort key.

$with_rank = (
select
    l_orderkey,
    l_returnflag,
    l_linestatus,
    l_shipdate,
    l_extendedprice,
    l_quantity,
    dense_rank() over (order by l_returnflag asc, l_shipdate asc) as date_rank
from
    `{path}lineitem`
);

-- Aggregate by rank to get daily statistics per returnflag
select
    date_rank,
    min(l_returnflag) as returnflag,
    min(l_shipdate) as shipdate,
    count(*) as cnt,
    sum(l_extendedprice) as total_price,
    sum(l_quantity) as total_qty,
    avg(l_extendedprice) as avg_price
from $with_rank
group by date_rank
order by date_rank
limit 200;
