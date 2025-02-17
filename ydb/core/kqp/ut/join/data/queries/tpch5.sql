-- TPC-H/TPC-R Local Supplier Volume Query (Q5)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$join1 = (
select
    o.o_orderkey as o_orderkey,
    o.o_orderdate as o_orderdate,
    c.c_nationkey as c_nationkey
from
    `/Root/customer` as c
join
    `/Root/orders` as o
on
    c.c_custkey = o.o_custkey
);

$join2 = (
select
    j.o_orderkey as o_orderkey,
    j.o_orderdate as o_orderdate,
    j.c_nationkey as c_nationkey,
    l.l_extendedprice as l_extendedprice,
    l.l_discount as l_discount,
    l.l_suppkey as l_suppkey
from
    $join1 as j
join
    `/Root/lineitem` as l
on
    l.l_orderkey = j.o_orderkey
);

$join3 = (
select
    j.o_orderkey as o_orderkey,
    j.o_orderdate as o_orderdate,
    j.c_nationkey as c_nationkey,
    j.l_extendedprice as l_extendedprice,
    j.l_discount as l_discount,
    j.l_suppkey as l_suppkey,
    s.s_nationkey as s_nationkey
from
    $join2 as j
join
    `/Root/supplier` as s
on
    j.l_suppkey = s.s_suppkey
);
$join4 = (
select
    j.o_orderkey as o_orderkey,
    j.o_orderdate as o_orderdate,
    j.c_nationkey as c_nationkey,
    j.l_extendedprice as l_extendedprice,
    j.l_discount as l_discount,
    j.l_suppkey as l_suppkey,
    j.s_nationkey as s_nationkey,
    n.n_regionkey as n_regionkey,
    n.n_name as n_name
from
    $join3 as j
join
    `/Root/nation` as n
on
    j.s_nationkey = n.n_nationkey
    and j.c_nationkey = n.n_nationkey
);
$join5 = (
select
    j.o_orderkey as o_orderkey,
    j.o_orderdate as o_orderdate,
    j.c_nationkey as c_nationkey,
    j.l_extendedprice as l_extendedprice,
    j.l_discount as l_discount,
    j.l_suppkey as l_suppkey,
    j.s_nationkey as s_nationkey,
    j.n_regionkey as n_regionkey,
    j.n_name as n_name,
    r.r_name as r_name
from
    $join4 as j
join
    `/Root/region` as r
on
    j.n_regionkey = r.r_regionkey
);
$border = Date('1995-01-01');
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    $join5
where
    r_name = 'AFRICA'
    and CAST(o_orderdate AS Timestamp) >= $border
    and CAST(o_orderdate AS Timestamp) < ($border + Interval('P365D'))
group by
    n_name
order by
    revenue desc;
