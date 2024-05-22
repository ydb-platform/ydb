-- TPC-H/TPC-R Local Supplier Volume Query (Q5)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$border = Date("1995-01-01");

$j1 = (
select
    n.n_name as n_name,
    n.n_nationkey as n_nationkey
from
    `{path}nation` as n
join
    `{path}region` as r
on
    n.n_regionkey = r.r_regionkey
where
    r_name = 'AFRICA'
);

$j2 = (
select
    j.n_name as n_name,
    j.n_nationkey as n_nationkey,
    s.s_suppkey as s_suppkey
from
    `{path}supplier` as s
join
    $j1 as j
on
    j.n_nationkey = s.s_nationkey
);

$j3 = (
select
    j.n_name as n_name,
    j.n_nationkey as n_nationkey,
    l.l_extendedprice as l_extendedprice,
    l.l_discount as l_discount,
    l.l_orderkey as l_orderkey
from
    `{path}lineitem` as l
join
    $j2 as j
on
    l.l_suppkey = j.s_suppkey
);

$j4 = (
select
    o.o_orderkey as o_orderkey,
    c.c_nationkey as c_nationkey
from
    `{path}orders` as o
join
    `{path}customer` as c
on
    c.c_custkey = o.o_custkey
where
    o.o_orderdate >= $border
    and o.o_orderdate < ($border + Interval("P365D"))
);

$j5 = (
select
    j3.n_name as n_name,
    j3.l_extendedprice as l_extendedprice,
    j3.l_discount as l_discount
from
    $j3 as j3
join
    $j4 as j4
on
    j3.n_nationkey = j4.c_nationkey
    and j3.l_orderkey = j4.o_orderkey
);

select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    $j5
group by
    n_name
order by
    revenue desc;
