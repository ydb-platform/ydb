-- TPC-H/TPC-R Returned Item Reporting Query (Q10)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$border = Date("1993-12-01");
$join1 = (
select
    c.c_custkey as c_custkey,
    c.c_name as c_name,
    c.c_acctbal as c_acctbal,
    c.c_address as c_address,
    c.c_phone as c_phone,
    c.c_comment as c_comment,
    c.c_nationkey as c_nationkey,
    o.o_orderkey as o_orderkey
from
    `{path}customer` as c
join
    `{path}orders` as o
on
    c.c_custkey = o.o_custkey
where
    o.o_orderdate >= $border
    and o.o_orderdate < ($border + Interval("P90D"))
);
$join2 = (
select
    j.c_custkey as c_custkey,
    j.c_name as c_name,
    j.c_acctbal as c_acctbal,
    j.c_address as c_address,
    j.c_phone as c_phone,
    j.c_comment as c_comment,
    j.c_nationkey as c_nationkey,
    l.l_extendedprice as l_extendedprice,
    l.l_discount as l_discount
from
    $join1 as j
join
    `{path}lineitem` as l
on
    l.l_orderkey = j.o_orderkey
where
    l.l_returnflag = 'R'
);
$join3 = (
select
    j.c_custkey as c_custkey,
    j.c_name as c_name,
    j.c_acctbal as c_acctbal,
    j.c_address as c_address,
    j.c_phone as c_phone,
    j.c_comment as c_comment,
    j.c_nationkey as c_nationkey,
    j.l_extendedprice as l_extendedprice,
    j.l_discount as l_discount,
    n.n_name as n_name
from
    $join2 as j
join
    `{path}nation` as n
on
    n.n_nationkey = j.c_nationkey
);
select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    $join3
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc
limit 20;
