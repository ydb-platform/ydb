{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Parts/Supplier Relationship Query (Q16)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
from
    {{partsupp}},
    {{part}}
where
    p_partkey = ps_partkey
    and p_brand <> 'Brand#33'
    and p_type not like 'PROMO POLISHED%'
    and p_size in (20, 27, 11, 45, 40, 41, 34, 36)
    and ps_suppkey not in (
        select
            s_suppkey
        from
            {{supplier}}
        where
            s_comment like '%Customer%Complaints%'
    )
group by
    p_brand,
    p_type,
    p_size
order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size;

