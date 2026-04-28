-- Sort Spilling Test: Sort orders by totalprice DESC, compute RANK
-- Uses status filter to reduce to ~1/3 of orders (~50M rows at scale 10000)
-- RANK() forces full sort by totalprice.

$filtered = (
select
    o_orderkey,
    o_custkey,
    o_totalprice,
    o_orderdate,
    o_orderpriority
from
    `column/tpch/s10000/orders`
where
    o_orderstatus = 'F'
);

$ranked = (
select
    o_orderkey,
    o_custkey,
    o_totalprice,
    o_orderdate,
    o_orderpriority,
    rank() over (order by o_totalprice desc) as price_rank
from $filtered
);

-- Get top-100 and sample every Nth row
select
    o_orderkey,
    o_custkey,
    o_totalprice,
    o_orderdate,
    o_orderpriority,
    price_rank
from $ranked
where price_rank <= 100 or price_rank % 500000 = 1
order by price_rank
limit 300;
