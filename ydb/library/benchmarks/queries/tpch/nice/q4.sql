{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Order Priority Checking Query (Q4)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    {{orders}}.o_orderpriority as o_orderpriority,
    count(*) as order_count
from
    {{orders}}
inner join (
    select distinct
        l_orderkey
    from
        {{lineitem}}
    where
        l_commitdate < l_receiptdate) as lineitem
on
    {{orders}}.o_orderkey = lineitem.l_orderkey
where
    o_orderdate >= date('1994-03-01')
    and o_orderdate < date('1994-03-01') + interval('P90D')
group by
    {{orders}}.o_orderpriority
order by
    o_orderpriority;
