{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Important Stock Identification Query (Q11)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    {{partsupp}},
    {{supplier}},
    {{nation}}
where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'CANADA'
group by
    ps_partkey having
        sum(ps_supplycost * ps_availqty) > (
            select
                sum(ps_supplycost * ps_availqty) * 0.0001000000
            from
                {{partsupp}},
                {{supplier}},
                {{nation}}
            where
                ps_suppkey = s_suppkey
                and s_nationkey = n_nationkey
                and n_name = 'CANADA'
        )
order by
    value desc;


