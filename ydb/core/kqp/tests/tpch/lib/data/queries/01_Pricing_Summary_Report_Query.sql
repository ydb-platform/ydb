--!syntax_v1

-- $ID$
-- TPC-H/TPC-R Pricing Summary Report Query (Q1)
-- Functional Query Definition
-- Approved February 1998

$PRAGMAS$

declare $start_date as Date;
declare $start_date_shift as Interval;

select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    `$DBROOT$/lineitem`
where
    -- CAST(l_shipdate AS Timestamp) <= (Date('1998-12-01') - Interval("P3D"))
    CAST(l_shipdate AS Timestamp) <= ($start_date - $start_date_shift)
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;
