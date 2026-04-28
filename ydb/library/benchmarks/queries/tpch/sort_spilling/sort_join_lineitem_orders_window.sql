-- Sort Spilling Test: Join lineitem with orders, sort, assign row numbers
-- Date filter reduces to ~1/7 of lineitem. Tests sort spilling after a join.

$joined = (
select
    l.l_orderkey as l_orderkey,
    l.l_extendedprice as l_extendedprice,
    l.l_shipdate as l_shipdate,
    o.o_orderdate as o_orderdate,
    o.o_totalprice as o_totalprice,
    o.o_orderpriority as o_orderpriority
from
    `column/tpch/s10000/lineitem` as l
join
    `column/tpch/s10000/orders` as o
on
    l.l_orderkey = o.o_orderkey
where
    l.l_shipdate >= Date('1995-01-01')
    and l.l_shipdate < Date('1996-01-01')
);

$numbered = (
select
    l_orderkey,
    l_extendedprice,
    l_shipdate,
    o_orderdate,
    o_totalprice,
    o_orderpriority,
    row_number() over (order by o_totalprice desc, l_shipdate asc) as rn
from $joined
);

-- Sample to verify sort order
select
    rn,
    l_orderkey,
    l_extendedprice,
    l_shipdate,
    o_totalprice,
    o_orderpriority
from $numbered
where rn % 1000000 = 1
order by rn
limit 200;
