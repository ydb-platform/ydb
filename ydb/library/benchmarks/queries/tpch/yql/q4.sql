{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Order Priority Checking Query (Q4)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$border = Date("1994-03-01");

$o = (
select
    o_orderpriority,
    o_orderkey
from
    {{orders}}
where
    o_orderdate >= $border
    and o_orderdate < DateTime::MakeDate(DateTime::ShiftMonths($border, 3))
);

$l = (
select
    distinct l_orderkey
from
    {{lineitem}}
where
    l_commitdate < l_receiptdate
);

select
    o.o_orderpriority as o_orderpriority,
    count(*) as order_count
from
    $o as o
join
    $l as l
on
    o.o_orderkey = l.l_orderkey
group by
    o.o_orderpriority
order by
    o_orderpriority;
