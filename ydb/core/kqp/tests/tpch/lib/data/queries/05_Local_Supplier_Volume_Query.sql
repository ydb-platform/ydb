-- $ID$
-- TPC-H/TPC-R Local Supplier Volume Query (Q5)
-- Functional Query Definition
-- Approved February 1998

$PRAGMAS$

$join1 = (
    select
        o.o_orderkey as o_orderkey,
        o.o_orderdate as o_orderdate,
        c.c_nationkey as c_nationkey
    from
        `$DBROOT$/orders` as o join `$DBROOT$/customer` as c on c.c_custkey = o.o_custkey
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
        `$DBROOT$/lineitem` as l join $join1 as j on l.l_orderkey = j.o_orderkey
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
        $join2 as j join `$DBROOT$/supplier` as s on j.l_suppkey = s.s_suppkey
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
        $join3 as j join `$DBROOT$/nation` as n
    on j.s_nationkey = n.n_nationkey
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
        $join4 as j join `$DBROOT$/region` as r on j.n_regionkey = r.r_regionkey
);

$border = Date("1996-01-01");
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    $join5
where
    r_name = 'AFRICA'
    and CAST(o_orderdate AS Timestamp) >= $border
    and CAST(o_orderdate AS Timestamp) < ($border + Interval("P365D"))
group by
    n_name
order by
    revenue desc;
