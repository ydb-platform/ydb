-- TPC-H/TPC-R Shipping Priority Query (Q3)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$c = (
select
    c_custkey
from
    `{path}customer`
where
    c_mktsegment = 'MACHINERY'
);

$o = (
select
    o_orderdate,
    o_shippriority,
    o_orderkey
from
    `{path}orders` as o
left semi join
    $c as c
on
    c.c_custkey = o.o_custkey
where
    o_orderdate < Date('1995-03-08')
);

$join2 = (
select
    o.o_orderdate as o_orderdate,
    o.o_shippriority as o_shippriority,
    l.l_orderkey as l_orderkey,
    l.l_discount as l_discount,
    l.l_extendedprice as l_extendedprice
from
    `{path}lineitem` as l
join
    $o as o
on
    l.l_orderkey = o.o_orderkey
where
    l_shipdate > Date('1995-03-08')
);

select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    $join2
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate
limit 10;
