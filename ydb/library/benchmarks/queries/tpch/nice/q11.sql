{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Important Stock Identification Query (Q11)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    {{partsupp}}.ps_partkey as ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    {{partsupp}}
    cross join {{supplier}}
    cross join {{nation}}
    cross join (
            select
                sum(ps_supplycost * ps_availqty) * 0.0001000000 as ps_threshold
            from
                {{partsupp}}
                cross join {{supplier}}
                cross join {{nation}}
            where
                ps_suppkey = s_suppkey
                and s_nationkey = n_nationkey
                and n_name = 'CANADA'
        ) as threshold
where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'CANADA'
group by
    {{partsupp}}.ps_partkey, threshold.ps_threshold having
        sum(ps_supplycost * ps_availqty) > threshold.ps_threshold
order by
    value desc;
