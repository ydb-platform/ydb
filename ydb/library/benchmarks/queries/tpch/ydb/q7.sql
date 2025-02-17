-- TPC-H/TPC-R Volume Shipping Query (Q7)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$n = select n_name, n_nationkey from `{path}nation` as n
    where n_name = 'PERU' or n_name = 'MOZAMBIQUE';

$l = select 
    l_orderkey, l_suppkey,
    DateTime::GetYear(cast(l_shipdate as timestamp)) as l_year,
    l_extendedprice * (1 - l_discount) as volume
from 
    `{path}lineitem` as l
where 
    l.l_shipdate between Date('1995-01-01') and Date('1996-12-31');

$j1 = select 
    n_name as supp_nation,
    s_suppkey
from 
    `{path}supplier` as supplier
join 
    $n as n1
on 
    supplier.s_nationkey = n1.n_nationkey;

$j2 = select
    n_name as cust_nation,
    c_custkey
from 
    `{path}customer` as customer
join 
    $n as n2
on 
    customer.c_nationkey = n2.n_nationkey;

$j3 = select
    cust_nation, o_orderkey
from 
    `{path}orders` as orders
join 
    $j2 as customer
on 
    orders.o_custkey = customer.c_custkey;

$j4 = select
    cust_nation,
    l_orderkey, l_suppkey,
    l_year,
    volume
from 
    $l as lineitem
join 
    $j3 as orders
on 
    lineitem.l_orderkey = orders.o_orderkey;

$j5 = select
    supp_nation, cust_nation,
    l_year, volume
from 
    $j4 as lineitem
join 
    $j1 as supplier
on 
    lineitem.l_suppkey = supplier.s_suppkey
where (supp_nation = 'PERU' and cust_nation = 'MOZAMBIQUE')
    OR (supp_nation = 'MOZAMBIQUE' and cust_nation = 'PERU');


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
