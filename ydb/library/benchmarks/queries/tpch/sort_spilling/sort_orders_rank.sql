-- Sort Spilling Test: Sort orders by totalprice DESC, compute RANK
-- TPC-H scale 10000: ~150M rows in orders
-- RANK() forces full sort by totalprice.

$ranked = (
select
    o_orderkey,
    o_custkey,
    o_totalprice,
    o_orderdate,
    o_orderpriority,
    rank() over (order by o_totalprice desc) as price_rank
from
    `column/tpch/s10000/orders`
);

-- Get top-100 and bottom-100 by rank
select * from (
    select
        o_orderkey,
        o_custkey,
        o_totalprice,
        o_orderdate,
        o_orderpriority,
        price_rank
    from $ranked
    where price_rank <= 100
    
    union all
    
    select
        o_orderkey,
        o_custkey,
        o_totalprice,
        o_orderdate,
        o_orderpriority,
        price_rank
    from $ranked
    where price_rank > 150000000 - 100  -- bottom 100 for scale 10000
)
order by price_rank
limit 200;
