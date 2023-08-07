-- TPC-H/TPC-R Volume Shipping Query (Q7)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$join1 = (
select
    l.l_extendedprice * (1 - l.l_discount) as volume,
    DateTime::GetYear(cast(l.l_shipdate as timestamp)) as l_year,
    l.l_orderkey as l_orderkey,
    s.s_nationkey as s_nationkey
from
    `{path}supplier` as s
join
    `{path}lineitem` as l
on
    s.s_suppkey = l.l_suppkey
where cast(cast(l.l_shipdate as Timestamp) as Date) between
    Date('1995-01-01')
    and Date('1996-12-31')
);
$join2 = (
select
    j.volume as volume,
    j.l_year as l_year,
    j.s_nationkey as s_nationkey,
    o.o_orderkey as o_orderkey,
    o.o_custkey as o_custkey
from
    $join1 as j
join
    `{path}orders` as o
on
    o.o_orderkey = j.l_orderkey
);

$join3 = (
select
    j.volume as volume,
    j.l_year as l_year,
    j.s_nationkey as s_nationkey,
    c.c_nationkey as c_nationkey
from
    $join2 as j
join
    `{path}customer` as c
on
    c.c_custkey = j.o_custkey
);

$join4 = (
select
    j.volume as volume,
    j.l_year as l_year,
    j.c_nationkey as c_nationkey,
    j.s_nationkey as s_nationkey,
    n.n_name as n_name
from
    $join3 as j
join
    `{path}nation` as n
on
    j.s_nationkey = n.n_nationkey
);
$join5 = (
select
    j.volume as volume,
    j.l_year as l_year,
    n.n_name as cust_nation,
    j.n_name as supp_nation
from
    $join4 as j
join
    `{path}nation` as n
on
    j.c_nationkey = n.n_nationkey
where (
    (n.n_name = 'PERU' and j.n_name = 'MOZAMBIQUE')
    or (n.n_name = 'MOZAMBIQUE' and j.n_name = 'PERU')
)
);

select
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) as revenue
from
    $join5 as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year;
