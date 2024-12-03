
-- TPC-H/TPC-R Global Sales Opportunity Query (Q22)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$customers = (
select
    c_acctbal,
    c_custkey,
    Substring(c_phone, 0u, 2u) as cntrycode
from
    plato.customer
where (Substring(c_phone, 0u, 2u) = '31' or Substring(c_phone, 0u, 2u) = '29' or Substring(c_phone, 0u, 2u) = '30' or Substring(c_phone, 0u, 2u) = '26' or Substring(c_phone, 0u, 2u) = '28' or Substring(c_phone, 0u, 2u) = '25' or Substring(c_phone, 0u, 2u) = '15')
);
$avg = (
select
    avg(c_acctbal) as a
from
    $customers
where
    c_acctbal > 0.00
);
$join1 = (
select
    c.c_acctbal as c_acctbal,
    c.c_custkey as c_custkey,
    c.cntrycode as cntrycode
from
    $customers as c
cross join
    $avg as a
where
    c.c_acctbal > a.a
);
$join2 = (
select
    j.cntrycode as cntrycode,
    c_custkey,
    j.c_acctbal as c_acctbal
from
    $join1 as j
left only join 
    plato.orders as o
on
    o.o_custkey = j.c_custkey
);

select
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
from
    $join2 as custsale
group by
    cntrycode
order by
    cntrycode;

