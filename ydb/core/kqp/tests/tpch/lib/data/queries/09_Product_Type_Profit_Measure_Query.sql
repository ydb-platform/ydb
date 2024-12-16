-- $ID$
-- TPC-H/TPC-R Product Type Profit Measure Query (Q9)
-- Functional Query Definition
-- Approved February 1998

$PRAGMAS$

$join1 = (
    select
        l.l_extendedprice as l_extendedprice,
        l.l_discount as l_discount,
        l.l_quantity as l_quantity,
        l.l_orderkey as l_orderkey,
        l.l_partkey as l_partkey,
        l.l_suppkey as l_suppkey,
        s.s_nationkey as s_nationkey
    from
        `$DBROOT$/lineitem` as l join `$DBROOT$/supplier` as s on s.s_suppkey = l.l_suppkey
);

$join2 = (
    select
        j.l_extendedprice as l_extendedprice,
        j.l_discount as l_discount,
        j.l_quantity as l_quantity,
        j.l_orderkey as l_orderkey,
        j.l_suppkey as l_suppkey,
        j.l_partkey as l_partkey,
        j.s_nationkey as s_nationkey
    from
        $join1 as j join `$DBROOT$/part` as p on p.p_partkey = j.l_partkey
    where
        String::Contains(p.p_name, 'papaya')
);

$join3 = (
    select
        j.l_orderkey as l_orderkey,
        j.l_extendedprice * (1 - j.l_discount) - ps.ps_supplycost * j.l_quantity as amount,
        j.s_nationkey as s_nationkey
    from
        $join2 as j join `$DBROOT$/partsupp` as ps
    on ps.ps_suppkey = j.l_suppkey
       and ps.ps_partkey = j.l_partkey
);

$join4 = (
    select
        j.amount as amount,
        DateTime::GetYear(o.o_orderdate) as o_year,
        j.s_nationkey as s_nationkey
    from
        $join3 as j join `$DBROOT$/orders` as o on o.o_orderkey = j.l_orderkey
);

$join5 = (
    select
        j.amount as amount,
        j.o_year as o_year,
        n.n_name as nation
    from
        $join4 as j join `$DBROOT$/nation` as n on n.n_nationkey = j.s_nationkey
);

select
    nation,
    o_year,
    sum(amount) as sum_profit
from
    $join5 as profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc
limit 10