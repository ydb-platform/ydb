-- $ID$
-- TPC-H/TPC-R Volume Shipping Query (Q7)
-- Functional Query Definition
-- Approved February 1998

$PRAGMAS$

$join1 = (
    select
        l.l_extendedprice * (1 - l.l_discount) as volume,
        DateTime::GetYear(l.l_shipdate) as l_year,
        l.l_orderkey as l_orderkey,
        s.s_nationkey as s_nationkey
    from
        `$DBROOT$/lineitem` as l join `$DBROOT$/supplier` as s on s.s_suppkey = l.l_suppkey
    where
        l.l_shipdate between Date('1995-01-01') and Date('1996-12-31')
);

$join2 = (
    select
        j.volume as volume,
        j.l_year as l_year,
        j.s_nationkey as s_nationkey,
        o.o_orderkey as o_orderkey,
        o.o_custkey as o_custkey
    from
        $join1 as j join `$DBROOT$/orders` as o on o.o_orderkey = j.l_orderkey
);

$join3 = (
    select
        j.volume as volume,
        j.l_year as l_year,
        j.s_nationkey as s_nationkey,
        c.c_nationkey as c_nationkey
    from
        $join2 as j join `$DBROOT$/customer` as c on c.c_custkey = j.o_custkey
);

$join4 = (
    select
        j.volume as volume,
        j.l_year as l_year,
        j.c_nationkey as c_nationkey,
        j.s_nationkey as s_nationkey,
        n.n_name as n_name
    from
        $join3 as j join `$DBROOT$/nation` as n on j.s_nationkey = n.n_nationkey
);

$join5 = (
    select
        j.volume as volume,
        j.l_year as l_year,
        n.n_name as cust_nation,
        j.n_name as supp_nation
    from
        $join4 as j join `$DBROOT$/nation` as n on j.c_nationkey = n.n_nationkey
    where
        (n.n_name = 'ALGERIA' and j.n_name = 'IRAN') or
        (n.n_name = 'CANADA' and j.n_name = 'ETHIOPIA')
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
