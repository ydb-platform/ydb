{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Suppliers Who Kept Orders Waiting Query (Q21)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    {{supplier}}.s_name as s_name,
    count(*) as numwait
from
    {{supplier}}
    cross join {{lineitem}} l1
    cross join {{orders}}
    cross join {{nation}}
    left semi join (
        select
            l2.l_orderkey as l_orderkey
        from
            {{lineitem}} l2
            cross join {{supplier}}
            cross join {{lineitem}} l1
            cross join {{orders}}
            cross join {{nation}}
        where
            s_suppkey = l1.l_suppkey
            and o_orderkey = l1.l_orderkey
            and o_orderstatus = 'F'
            and l1.l_receiptdate > l1.l_commitdate
            and s_nationkey = n_nationkey
            and n_name = 'EGYPT'
            and l2.l_orderkey = l1.l_orderkey
            and l2.l_suppkey <> l1.l_suppkey
    ) as l2 on l2.l_orderkey = l1.l_orderkey
    left only join (
        select
            l3.l_orderkey as l_orderkey
        from
            {{lineitem}} l3
            cross join {{supplier}}
            cross join {{lineitem}} l1
            cross join {{orders}}
            cross join {{nation}}
        where
            s_suppkey = l1.l_suppkey
            and o_orderkey = l1.l_orderkey
            and o_orderstatus = 'F'
            and l1.l_receiptdate > l1.l_commitdate
            and s_nationkey = n_nationkey
            and n_name = 'EGYPT'
            and l3.l_orderkey = l1.l_orderkey
            and l3.l_suppkey <> l1.l_suppkey
            and l3.l_receiptdate > l3.l_commitdate
    ) as l3 on l3.l_orderkey = l1.l_orderkey
where
    s_suppkey = l1.l_suppkey
    and o_orderkey = l1.l_orderkey
    and o_orderstatus = 'F'
    and l1.l_receiptdate > l1.l_commitdate
    and s_nationkey = n_nationkey
    and n_name = 'EGYPT'
group by
    {{supplier}}.s_name
order by
    numwait desc,
    s_name
limit 100;
