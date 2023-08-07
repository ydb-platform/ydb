{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Order Priority Checking Query (Q4)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    o_orderpriority,
    count(*) as order_count
from
    {{orders}}
where
    o_orderdate >= date '1994-03-01'
    and o_orderdate < date '1994-03-01' + interval '3' month
    and exists (
        select
            *
        from
            {{lineitem}}
        where
            l_orderkey = o_orderkey
            and l_commitdate < l_receiptdate
    )
group by
    o_orderpriority
order by
    o_orderpriority;


