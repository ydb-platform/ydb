-- Sort Spilling Test: Sort lineitem by returnflag+shipdate, compute DENSE_RANK
-- Uses date filter to reduce to ~1/7 of lineitem.
-- Tests dense ranking over large dataset with composite sort key.

PRAGMA ydb.DisableBlockExecution;

$filtered = (
select
    l_orderkey,
    l_returnflag,
    l_linestatus,
    l_shipdate,
    l_extendedprice,
    l_quantity
from
    `column/tpch/s10000/lineitem`
where
    l_shipdate >= Date('1996-01-01')
    and l_shipdate < Date('1997-01-01')
);

$with_rank = (
select
    l_orderkey,
    l_returnflag,
    l_linestatus,
    l_shipdate,
    l_extendedprice,
    l_quantity,
    dense_rank() over (order by l_returnflag asc, l_shipdate asc) as date_rank
from $filtered
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
