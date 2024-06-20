               $join1 = (
select
    c.c_mktsegment as c_mktsegment,
    o.o_orderdate as o_orderdate,
    o.o_shippriority as o_shippriority,
    o.o_orderkey as o_orderkey
from
     `/Root/customer` as c
join
     `/Root/orders` as o
on
    c.c_custkey = o.o_custkey
);

$join2 = (
select
    j1.c_mktsegment as c_mktsegment,
    j1.o_orderdate as o_orderdate,
    j1.o_shippriority as o_shippriority,
    l.l_orderkey as l_orderkey,
    l.l_discount as l_discount,
    l.l_shipdate as l_shipdate,
    l.l_extendedprice as l_extendedprice
from
    $join1 as j1
join
    `/Root/lineitem` as l
on
    l.l_orderkey = j1.o_orderkey
);

select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    $join2
where
    c_mktsegment = 'MACHINERY'
    and CAST(o_orderdate AS Timestamp) < Date('1995-03-08')
    and CAST(l_shipdate AS Timestamp) > Date('1995-03-08')
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate
limit 10;
