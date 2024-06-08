{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Parts/Supplier Relationship Query (Q16)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$p = (
select
    p.p_brand as p_brand,
    p.p_type as p_type,
    p.p_size as p_size,
    ps.ps_suppkey as ps_suppkey
from
    {{part}} as p
join
    {{partsupp}} as ps
on
    p.p_partkey = ps.ps_partkey
where
    p.p_brand <> 'Brand#33'
    and p.p_type not like 'PROMO POLISHED%'
    and (p.p_size = 20 or p.p_size = 27 or p.p_size = 11 or p.p_size = 45 or p.p_size = 40 or p.p_size = 41 or p.p_size = 34 or p.p_size = 36)
);

$s = (
select
    s_suppkey
from
    {{supplier}}
where
    s_comment like "%Customer%Complaints%"
);

$j = (
select
    p.p_brand as p_brand,
    p.p_type as p_type,
    p.p_size as p_size,
    p.ps_suppkey as ps_suppkey
from
    $p as p
left only join
    $s as s
on
    p.ps_suppkey = s.s_suppkey
);

select
    j.p_brand as p_brand,
    j.p_type as p_type,
    j.p_size as p_size,
    count(distinct j.ps_suppkey) as supplier_cnt
from
    $j as j
group by
    j.p_brand,
    j.p_type,
    j.p_size
order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size
;
