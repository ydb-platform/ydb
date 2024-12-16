-- $ID$
-- TPC-H/TPC-R Returned Item Reporting Query (Q10)
-- Functional Query Definition
-- Approved February 1998

$PRAGMAS$

$border = Date("1993-02-01");

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
        `$DBROOT$/orders` as o join `$DBROOT$/customer` as c on c.c_custkey = o.o_custkey
    where
        cast(o.o_orderdate as timestamp) >= $border and
        cast(o.o_orderdate as timestamp) < ($border + Interval("P93D"))
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
        `$DBROOT$/lineitem` as l join $join1 as j on l.l_orderkey = j.o_orderkey
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
        $join2 as j join `$DBROOT$/nation` as n on n.n_nationkey = j.c_nationkey
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
