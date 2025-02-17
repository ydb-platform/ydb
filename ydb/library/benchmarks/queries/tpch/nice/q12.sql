{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Shipping Modes and Order Priority Query (Q12)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    {{lineitem}}.l_shipmode,
    sum(case
        when o_orderpriority = '1-URGENT'
            or o_orderpriority = '2-HIGH'
            then 1
        else 0
    end) as high_line_count,
    sum(case
        when o_orderpriority <> '1-URGENT'
            and o_orderpriority <> '2-HIGH'
            then 1
        else 0
    end) as low_line_count
from
    {{orders}}
    cross join {{lineitem}}
where
    o_orderkey = l_orderkey
    and l_shipmode in ('MAIL', 'TRUCK')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= date('1994-01-01')
    and l_receiptdate < date('1994-01-01') + interval('P365D')
group by
    {{lineitem}}.l_shipmode
order by
    {{lineitem}}.l_shipmode;


