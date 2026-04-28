-- Sort Spilling Test: Sort lineitem by extendedprice DESC, assign row numbers
-- Uses returnflag filter to reduce dataset to ~1/3 (~200M rows at scale 10000)
-- ROW_NUMBER() forces full sort. We then filter to get percentile boundaries.

$filtered = (
select
    l_orderkey,
    l_partkey,
    l_extendedprice,
    l_shipdate
from
    `column/tpch/s10000/lineitem`
where
    l_returnflag = 'R'
);

$numbered = (
select
    l_orderkey,
    l_partkey,
    l_extendedprice,
    l_shipdate,
    row_number() over (order by l_extendedprice desc) as rn
from $filtered
);

-- Extract percentile boundaries
select
    rn,
    l_orderkey,
    l_partkey,
    l_extendedprice,
    l_shipdate
from $numbered
where rn % 2000000 = 1
order by rn
limit 200;
