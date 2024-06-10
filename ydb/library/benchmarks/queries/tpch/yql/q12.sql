{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Shipping Modes and Order Priority Query (Q12)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$join = (
    select
        l.l_shipmode as l_shipmode,
        o.o_orderpriority as o_orderpriority,
        l.l_commitdate as l_commitdate,
        l.l_shipdate as l_shipdate,
        l.l_receiptdate as l_receiptdate
    from
        {{orders}} as o
        join {{lineitem}} as l
        on o.o_orderkey = l.l_orderkey
);

$border = Date("1994-01-01");

select
    l_shipmode,
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
from $join
where
    (l_shipmode = 'MAIL' or l_shipmode = 'TRUCK')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= $border
    and l_receiptdate < ($border + Interval("P365D"))
group by
    l_shipmode
order by
    l_shipmode;
