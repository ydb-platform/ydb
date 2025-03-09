PRAGMA ydb.OptShuffleElimination = 'true';

$z0_12 = 0.;
$customers = (
select
    c_acctbal,
    c_custkey,
    c_phone as cntrycode
from
    customer
);
$c = (
select
    c_acctbal,
    c_custkey,
    cntrycode
from
    $customers
);
$avg = (
select
    avg(c_acctbal) as a
from
    $c
where
    c_acctbal > $z0_12
);
$join1 = (
select
    c.c_acctbal as c_acctbal,
    c.c_custkey as c_custkey,
    c.cntrycode as cntrycode
from
    $c as c
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
    orders as o
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