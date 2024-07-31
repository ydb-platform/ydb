{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Pricing Summary Report Query (Q1)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * ($z1_12 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * ($z1_12 - l_discount) * ($z1_12 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    {{lineitem}}
where
    l_shipdate <= Date('1998-12-01') - Interval("P100D")
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;
