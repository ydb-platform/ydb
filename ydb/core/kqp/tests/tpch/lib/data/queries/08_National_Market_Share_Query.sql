-- $ID$
-- TPC-H/TPC-R National Market Share Query (Q8)
-- Functional Query Definition
-- Approved February 1998

$PRAGMAS$

$join1 = (
    select
        l.l_extendedprice * (1 - l.l_discount) as volume,
        l.l_suppkey as l_suppkey,
        l.l_orderkey as l_orderkey
    from
        `$DBROOT$/lineitem` as l join `$DBROOT$/part` as p on p.p_partkey = l.l_partkey
    where
        p.p_type = 'LARGE BRUSHED COPPER'
);

$join2 = (
    select
        j.volume as volume,
        j.l_orderkey as l_orderkey,
        s.s_nationkey as s_nationkey
    from
        $join1 as j join `$DBROOT$/supplier` as s on s.s_suppkey = j.l_suppkey
);

$join3 = (
    select
        j.volume as volume,
        j.l_orderkey as l_orderkey,
        n.n_name as nation
    from
        $join2 as j join `$DBROOT$/nation` as n on n.n_nationkey = j.s_nationkey
);

$join4 = (
    select
        j.volume as volume,
        j.nation as nation,
        DateTime::GetYear(o.o_orderdate) as o_year,
        o.o_custkey as o_custkey
    from
        $join3 as j join `$DBROOT$/orders` as o on o.o_orderkey = j.l_orderkey
);

$join5 = (
    select
        j.volume as volume,
        j.nation as nation,
        j.o_year as o_year,
        c.c_nationkey as c_nationkey
    from
        $join4 as j join `$DBROOT$/customer` as c on c.c_custkey = j.o_custkey
);

$join6 = (
    select
        j.volume as volume,
        j.nation as nation,
        j.o_year as o_year,
        n.n_regionkey as n_regionkey
    from
        $join5 as j join `$DBROOT$/nation` as n on n.n_nationkey = j.c_nationkey
);

$join7 = (
    select
        j.volume as volume,
        j.nation as nation,
        j.o_year as o_year
    from
        $join6 as j join `$DBROOT$/region` as r on r.r_regionkey = j.n_regionkey
    where
        r.r_name = 'MIDDLE EAST'
);

select
    o_year,
    sum(case
            when nation = 'PERU' then volume
            else 0
        end) / sum(volume) as mkt_share
from
    $join7 as all_nations
group by
    o_year
order by
    o_year;
