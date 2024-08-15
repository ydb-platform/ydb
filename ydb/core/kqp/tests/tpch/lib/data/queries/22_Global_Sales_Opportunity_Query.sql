-- $ID$
-- TPC-H/TPC-R Global Sales Opportunity Query (Q22)
-- Functional Query Definition
-- Approved February 1998

$PRAGMAS$

$customers = (
    select
        c_acctbal,
        c_custkey,
        substring(tobytes(c_phone), 0, 2) as cntrycode
    from
        `$DBROOT$/customer`
    where
        substring(tobytes(c_phone), 0, 2) in aslist('22', '10', '13', '19', '15', '32', '26')
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
        $customers as c cross join $avg as a
    where
        c.c_acctbal > a.a
);

$join2 = (
    select
        j.cntrycode as cntrycode,
        j.c_acctbal as c_acctbal
    from
        $join1 as j left only join `$DBROOT$/orders` as o on o.o_custkey = j.c_custkey
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
