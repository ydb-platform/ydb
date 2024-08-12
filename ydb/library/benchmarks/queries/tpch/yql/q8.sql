{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R National Market Share Query (Q8)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$join1 = (
select
    l.l_extendedprice * ($z1_12 - l.l_discount) as volume,
    l.l_suppkey as l_suppkey,
    l.l_orderkey as l_orderkey
from
    {{part}} as p
join
    {{lineitem}} as l
on
    p.p_partkey = l.l_partkey
where
    p.p_type = 'ECONOMY PLATED COPPER'
);
$join2 = (
select
    j.volume as volume,
    j.l_orderkey as l_orderkey,
    s.s_nationkey as s_nationkey
from
    $join1 as j
join
    {{supplier}} as s
on
    s.s_suppkey = j.l_suppkey
);
$join3 = (
select
    j.volume as volume,
    j.l_orderkey as l_orderkey,
    n.n_name as nation
from
    $join2 as j
join
    {{nation}} as n
on
    n.n_nationkey = j.s_nationkey
);
$join4 = (
select
    j.volume as volume,
    j.nation as nation,
    DateTime::GetYear(cast(o.o_orderdate as Timestamp)) as o_year,
    o.o_custkey as o_custkey
from
    $join3 as j
join
    {{orders}} as o
on
    o.o_orderkey = j.l_orderkey
where o_orderdate between Date('1995-01-01') and Date('1996-12-31')
);
$join5 = (
select
    j.volume as volume,
    j.nation as nation,
    j.o_year as o_year,
    c.c_nationkey as c_nationkey
from
    $join4 as j
join
    {{customer}} as c
on
    c.c_custkey = j.o_custkey
);
$join6 = (
select
    j.volume as volume,
    j.nation as nation,
    j.o_year as o_year,
    n.n_regionkey as n_regionkey
from
    $join5 as j
join
    {{nation}} as n
on
    n.n_nationkey = j.c_nationkey
);
$join7 = (
select
    j.volume as volume,
    j.nation as nation,
    j.o_year as o_year
from
    $join6 as j
join
    {{region}} as r
on
    r.r_regionkey = j.n_regionkey
where
    r.r_name = 'AFRICA'
);

select
    o_year,
    sum(case
        when nation = 'MOZAMBIQUE' then volume
        else $z0_12
    end) / sum(volume) as mkt_share
from
    $join7 as all_nations
group by
    o_year
order by
    o_year;
