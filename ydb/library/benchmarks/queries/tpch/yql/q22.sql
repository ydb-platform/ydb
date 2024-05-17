{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Global Sales Opportunity Query (Q22)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$phone_code = ($c_phone) -> {
    RETURN (Substring(CAST($c_phone AS String), 0u, 2u));
};

$customers = (
select
    c_acctbal,
    c_custkey,
    $phone_code(c_phone) as cntrycode
from
    {{customer}}
where ($phone_code(c_phone) = '31' or $phone_code(c_phone) = '29' or $phone_code(c_phone) = '30' or $phone_code(c_phone) = '26' or $phone_code(c_phone) = '28' or $phone_code(c_phone) = '25' or $phone_code(c_phone) = '15')
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
    {{orders}} as o
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

