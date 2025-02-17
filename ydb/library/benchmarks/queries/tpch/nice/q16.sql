{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Parts/Supplier Relationship Query (Q16)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    {{part}}.p_brand as p_brand,
    {{part}}.p_type as p_type,
    {{part}}.p_size as p_size,
    count(distinct {{partsupp}}.ps_suppkey) as supplier_cnt
from
    {{partsupp}}
    cross join {{part}}
    left only join (
        select
            s_suppkey
        from
            {{supplier}}
        where
            s_comment like '%Customer%Complaints%'
    ) as supplier
    on
        {{partsupp}}.ps_suppkey = supplier.s_suppkey
where
    p_partkey = ps_partkey
    and p_brand <> 'Brand#33'
    and p_type not like 'PROMO POLISHED%'
    and p_size in (20, 27, 11, 45, 40, 41, 34, 36)
group by
    {{part}}.p_brand,
    {{part}}.p_type,
    {{part}}.p_size
order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size;
