{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Small-Quantity-Order Revenue Query (Q17)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    sum(l_extendedprice) / 7.0 as avg_yearly
from
    {{lineitem}}
    cross join {{part}}
    cross join (
        select
            l_partkey,
            0.2 * avg(l_quantity) as quantity_threshold
        from
            {{lineitem}}
        group by 
            l_partkey
    ) as threshold
where
    {{part}}.p_partkey = {{lineitem}}.l_partkey
    and p_brand = 'Brand#35'
    and p_container = 'LG DRUM'
    and l_quantity < quantity_threshold
    and {{part}}.p_partkey = threshold.l_partkey;
