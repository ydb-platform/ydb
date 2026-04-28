-- Sort Spilling Test: Sort lineitem by extendedprice DESC, assign row numbers
-- TPC-H scale 10000: ~600M rows in lineitem
-- ROW_NUMBER() forces full sort. We then filter to get percentile boundaries.

$numbered = (
select
    l_orderkey,
    l_partkey,
    l_extendedprice,
    l_shipdate,
    row_number() over (order by l_extendedprice desc) as rn
from
    `column/tpch/s10000/lineitem`
);

-- Extract percentile boundaries (every 1% of data)
select
    rn,
    l_orderkey,
    l_partkey,
    l_extendedprice,
    l_shipdate
from $numbered
where rn % 6000000 = 1  -- ~100 rows for scale 10000 (600M / 6M)
order by rn
limit 200;
