-- $ID$
-- TPC-H/TPC-R Potential Part Promotion Query (Q20)
-- Function Query Definition
-- Approved February 1998

$PRAGMAS$

$border = Date("1993-01-01");

$threshold = (
    select
        0.5 * sum(l_quantity) as threshold,
        l_partkey as l_partkey,
        l_suppkey as l_suppkey
    from
        `$DBROOT$/lineitem`
    where
        cast(l_shipdate as timestamp) >= $border
        and cast(l_shipdate as timestamp) < ($border + Interval("P365D"))
    group by
        l_partkey, l_suppkey
);

$parts = (
    select
        p_partkey
    from
        `$DBROOT$/part`
    where
        StartsWith(p_name, 'aqu')
);

$join1 = (
    select
        ps.ps_suppkey as ps_suppkey,
        ps.ps_availqty as ps_availqty,
        ps.ps_partkey as ps_partkey
    from
        `$DBROOT$/partsupp` as ps join $parts as p on ps.ps_partkey = p.p_partkey
);

$join2 = (
    select
        j.ps_suppkey as ps_suppkey
    from
        $join1 as j join $threshold as t on j.ps_partkey = t.l_partkey and j.ps_suppkey = t.l_suppkey
    where
        j.ps_availqty > t.threshold
);

$join3 = (
    select
        j.ps_suppkey as ps_suppkey,
        s.s_name as s_name,
        s.s_address as s_address,
        s.s_nationkey as s_nationkey
    from
        $join2 as j join `$DBROOT$/supplier` as s on j.ps_suppkey = s.s_suppkey
);

select
    j.s_name as s_name,
    j.s_address as s_address
from
    $join3 as j join `$DBROOT$/nation` as n on j.s_nationkey = n.n_nationkey
where
    n.n_name = 'PERU'
order by
    s_name;
