-- Sort Spilling Test: Sort lineitem subset by shipdate, compute running sum
-- Uses a date filter to reduce dataset to ~1/7 of lineitem (~85M rows at scale 10000)
-- The window function forces a full WideSort that cannot be replaced with TopSort.

$filtered = (
select
    l_orderkey,
    l_shipdate,
    l_quantity,
    l_extendedprice
from
    `column/tpch/s10000/lineitem`
where
    l_shipdate >= Date('1997-01-01')
    and l_shipdate < Date('1998-01-01')
);

$sorted = (
select
    l_orderkey,
    l_shipdate,
    l_quantity,
    l_extendedprice,
    row_number() over (order by l_shipdate, l_orderkey) as rn
from $filtered
);

-- Sample every Nth row to verify sort order without materializing everything
select
    rn,
    l_orderkey,
    l_shipdate,
    l_quantity,
    l_extendedprice
from $sorted
where rn % 1000000 = 1
order by rn
limit 200;
