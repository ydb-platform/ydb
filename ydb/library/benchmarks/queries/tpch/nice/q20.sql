{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Potential Part Promotion Query (Q20)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    s_name,
    s_address
from
    {{supplier}}
    cross join {{nation}}
    cross join (
        select
            ps_suppkey
        from
            {{partsupp}}
            cross join {{part}}
            cross join (
                select
                    l_partkey,
                    l_suppkey,
                    0.5 * sum(l_quantity) as q_threshold
                from
                    {{lineitem}}
                where
                    l_shipdate >= date('1993-01-01')
                    and l_shipdate < date('1993-01-01') + interval('P365D')
                group by
                    l_partkey,
                    l_suppkey
            ) as threshold
        where
            ps_partkey = p_partkey
            and ps_partkey = l_partkey
            and ps_suppkey = l_suppkey
            and p_name like 'maroon%'
            and ps_availqty > threshold.q_threshold
    ) as partsupp
where
    s_suppkey = ps_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'VIETNAM'
order by
    s_name;
