-- Sort Spilling Test: Sort orders by orderdate, assign row numbers
-- ~150M rows at scale 10000. ROW_NUMBER forces full sort.

$numbered = (
select
    o_orderkey,
    o_orderdate,
    o_totalprice,
    o_orderstatus,
    row_number() over (order by o_orderdate, o_orderkey) as rn
from
    `column/tpch/s10000/orders`
);

-- Sample every Nth row to verify sort order
select
    rn,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    o_orderstatus
from $numbered
where rn % 1500000 = 1
order by rn
limit 200;
