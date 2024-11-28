
-- TPC-H/TPC-R Parts/Supplier Relationship Query (Q16)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$join = (
select
    ps.ps_suppkey as ps_suppkey,
    ps.ps_partkey as ps_partkey
from
    plato.partsupp as ps
left join
    plato.supplier as w
on
    w.s_suppkey = ps.ps_suppkey
where not (s_comment like "%Customer%Complaints%")
);

select
    p.p_brand as p_brand,
    p.p_type as p_type,
    p.p_size as p_size,
    count(distinct j.ps_suppkey) as supplier_cnt
from
    $join as j
join
    plato.part as p
on
    p.p_partkey = j.ps_partkey
where
    p.p_brand <> 'Brand#33'
    and (not StartsWith(p.p_type, 'PROMO POLISHED'))
    and (p.p_size = 20 or p.p_size = 27 or p.p_size = 11 or p.p_size = 45 or p.p_size = 40 or p.p_size = 41 or p.p_size = 34 or p.p_size = 36)
group by
    p.p_brand,
    p.p_type,
    p.p_size
order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size
;

