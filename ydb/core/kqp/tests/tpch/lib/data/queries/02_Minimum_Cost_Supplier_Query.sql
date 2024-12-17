--!syntax_v1

-- $ID$
-- TPC-H/TPC-R Minimum Cost Supplier Query (Q2)
-- Functional Query Definition
-- Approved February 1998

$PRAGMAS$

$join1 = (
    select
        ps.ps_supplycost as ps_supplycost,
        ps.ps_suppkey as ps_suppkey,
        p.p_type as p_type,
        p.p_partkey as p_partkey,
        p.p_size as p_size,
        p.p_mfgr as p_mfgr
    from
        `$DBROOT$/partsupp` as ps join `$DBROOT$/part` as p on ps.ps_partkey = p.p_partkey
);

$join2 = (
    select
        j1.ps_supplycost as ps_supplycost,
        j1.ps_suppkey as ps_suppkey,
        j1.p_type as p_type,
        j1.p_partkey as p_partkey,
        j1.p_size as p_size,
        j1.p_mfgr as p_mfgr,
        s.s_address as s_address,
        s.s_nationkey as s_nationkey,
        s.s_acctbal as s_acctbal,
        s.s_name as s_name,
        s.s_phone as s_phone,
        s.s_comment as s_comment
    from
        $join1 as j1 join `$DBROOT$/supplier` as s on j1.ps_suppkey = s.s_suppkey
);

$join3 = (
    select
        j2.ps_supplycost as ps_supplycost,
        j2.ps_suppkey as ps_suppkey,
        j2.p_type as p_type,
        j2.p_partkey as p_partkey,
        j2.p_size as p_size,
        j2.p_mfgr as p_mfgr,
        j2.s_address as s_address,
        j2.s_nationkey as s_nationkey,
        j2.s_acctbal as s_acctbal,
        j2.s_name as s_name,
        j2.s_phone as s_phone,
        j2.s_comment as s_comment,
        n.n_name as n_name,
        n.n_regionkey as n_regionkey
    from
        $join2 as j2 join `$DBROOT$/nation` as n on j2.s_nationkey = n.n_nationkey
);

$join4 = (
    select
        j3.ps_supplycost as ps_supplycost,
        j3.ps_suppkey as ps_suppkey,
        j3.p_type as p_type,
        j3.p_partkey as p_partkey,
        j3.p_size as p_size,
        j3.p_mfgr as p_mfgr,
        j3.s_address as s_address,
        j3.s_nationkey as s_nationkey,
        j3.s_acctbal as s_acctbal,
        j3.s_name as s_name,
        j3.s_phone as s_phone,
        j3.s_comment as s_comment,
        j3.n_name as n_name,
        j3.n_regionkey as n_regionkey,
        r.r_name as r_name
    from
        $join3 as j3 join `$DBROOT$/region` as r on j3.n_regionkey = r.r_regionkey
);

$min_supplycost = (
    select
        min(ps_supplycost) as min_supplycost
    from
        $join4
    where
        p_size = 9
        and String::HasSuffix(p_type, 'COPPER')
        and r_name = 'EUROPE'
);

$join5 = (
    select
        j4.ps_supplycost as ps_supplycost,
        j4.ps_suppkey as ps_suppkey,
        j4.p_type as p_type,
        j4.p_partkey as p_partkey,
        j4.p_size as p_size,
        j4.p_mfgr as p_mfgr,
        j4.s_address as s_address,
        j4.s_nationkey as s_nationkey,
        j4.s_acctbal as s_acctbal,
        j4.s_name as s_name,
        j4.s_phone as s_phone,
        j4.s_comment as s_comment,
        j4.n_name as n_name,
        j4.n_regionkey as n_regionkey,
        j4.r_name as r_name
    from
        $join4 as j4 join $min_supplycost as m on m.min_supplycost = j4.ps_supplycost
);

select
    s_acctbal as s_acctbal,
    s_name as s_name,
    n_name as n_name,
    p_partkey as p_partkey,
    p_mfgr as p_mfgr,
    s_address as s_address,
    s_phone as s_phone,
    s_comment as s_comment
from
    $join5
where
    p_size = 9
    and String::HasSuffix(p_type, 'COPPER')
    and r_name = 'EUROPE'
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
limit 100;
