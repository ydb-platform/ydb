-- TPC-H/TPC-R Pricing Summary Report Query (Q1)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

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
    `{path}lineitem`
where
    CAST(l_shipdate AS Timestamp) <= (CAST('1998-12-01' AS Date) - Interval("P100D"))
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;
-- end query
-- TPC-H/TPC-R Minimum Cost Supplier Query (Q2)
-- using 1680793381 as a seed to the RNG

$r = (select r_regionkey from
    `{path}region`
where r_name='AMERICA');

$j1 = (select n_name,n_nationkey
    from `{path}nation` as n
    join $r as r on
    n.n_regionkey = r.r_regionkey);

$j2 = (select s_acctbal,s_name,s_address,s_phone,s_comment,n_name,s_suppkey
    from `{path}supplier` as s
    join $j1 as j on
    s.s_nationkey = j.n_nationkey
);

$j3 = (select ps_partkey,ps_supplycost,s_acctbal,s_name,s_address,s_phone,s_comment,n_name
    from `{path}partsupp` as ps
    join $j2 as j on
    ps.ps_suppkey = j.s_suppkey
);

$min_ps_supplycost = (select min(ps_supplycost) as min_ps_supplycost,ps_partkey
    from $j3
    group by ps_partkey
);

$p = (select p_partkey,p_mfgr
    from `{path}part`
    where
    p_size = 10
    and p_type like '%COPPER'
);

$j4 = (select s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
    from $p as p
    join $j3 as j on p.p_partkey = j.ps_partkey
    join $min_ps_supplycost as m on p.p_partkey = m.ps_partkey
    where min_ps_supplycost=ps_supplycost
);

select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from $j4
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
limit 100;

-- end query
-- TPC-H/TPC-R Shipping Priority Query (Q3)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$join1 = (
select
    c.c_mktsegment as c_mktsegment,
    o.o_orderdate as o_orderdate,
    o.o_shippriority as o_shippriority,
    o.o_orderkey as o_orderkey
from
    `{path}customer` as c
join
    `{path}orders` as o
on
    c.c_custkey = o.o_custkey
);

$join2 = (
select
    j1.c_mktsegment as c_mktsegment,
    j1.o_orderdate as o_orderdate,
    j1.o_shippriority as o_shippriority,
    l.l_orderkey as l_orderkey,
    l.l_discount as l_discount,
    l.l_shipdate as l_shipdate,
    l.l_extendedprice as l_extendedprice
from
    $join1 as j1
join
    `{path}lineitem` as l
on
    l.l_orderkey = j1.o_orderkey
);

select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    $join2
where
    c_mktsegment = 'MACHINERY'
    and CAST(o_orderdate AS Timestamp) < Date('1995-03-08')
    and CAST(l_shipdate AS Timestamp) > Date('1995-03-08')
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate
limit 10;
-- end query
-- TPC-H/TPC-R Order Priority Checking Query (Q4)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$border = Date("1994-03-01");

$join = (select
        o.o_orderpriority as o_orderpriority,
        o.o_orderdate as o_orderdate,
        l.l_commitdate as l_commitdate,
        l.l_receiptdate as l_receiptdate
    from
        `{path}orders` as o
        join any `{path}lineitem` as l
        on o.o_orderkey = l.l_orderkey);

select
    o_orderpriority,
    count(*) as order_count
from $join
where
    CAST(o_orderdate AS Timestamp) >= $border
    and CAST(o_orderdate AS Timestamp) < DateTime::MakeDate(DateTime::ShiftMonths($border, 3))
    and l_commitdate < l_receiptdate
group by
    o_orderpriority
order by
    o_orderpriority;
-- end query
-- TPC-H/TPC-R Local Supplier Volume Query (Q5)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$join1 = (
select
    o.o_orderkey as o_orderkey,
    o.o_orderdate as o_orderdate,
    c.c_nationkey as c_nationkey
from
    `{path}customer` as c
join
    `{path}orders` as o
on
    c.c_custkey = o.o_custkey
);

$join2 = (
select
    j.o_orderkey as o_orderkey,
    j.o_orderdate as o_orderdate,
    j.c_nationkey as c_nationkey,
    l.l_extendedprice as l_extendedprice,
    l.l_discount as l_discount,
    l.l_suppkey as l_suppkey
from
    $join1 as j
join
    `{path}lineitem` as l
on
    l.l_orderkey = j.o_orderkey
);

$join3 = (
select
    j.o_orderkey as o_orderkey,
    j.o_orderdate as o_orderdate,
    j.c_nationkey as c_nationkey,
    j.l_extendedprice as l_extendedprice,
    j.l_discount as l_discount,
    j.l_suppkey as l_suppkey,
    s.s_nationkey as s_nationkey
from
    $join2 as j
join
    `{path}supplier` as s
on
    j.l_suppkey = s.s_suppkey
);
$join4 = (
select
    j.o_orderkey as o_orderkey,
    j.o_orderdate as o_orderdate,
    j.c_nationkey as c_nationkey,
    j.l_extendedprice as l_extendedprice,
    j.l_discount as l_discount,
    j.l_suppkey as l_suppkey,
    j.s_nationkey as s_nationkey,
    n.n_regionkey as n_regionkey,
    n.n_name as n_name
from
    $join3 as j
join
    `{path}nation` as n
on
    j.s_nationkey = n.n_nationkey
    and j.c_nationkey = n.n_nationkey
);
$join5 = (
select
    j.o_orderkey as o_orderkey,
    j.o_orderdate as o_orderdate,
    j.c_nationkey as c_nationkey,
    j.l_extendedprice as l_extendedprice,
    j.l_discount as l_discount,
    j.l_suppkey as l_suppkey,
    j.s_nationkey as s_nationkey,
    j.n_regionkey as n_regionkey,
    j.n_name as n_name,
    r.r_name as r_name
from
    $join4 as j
join
    `{path}region` as r
on
    j.n_regionkey = r.r_regionkey
);
$border = Date("1995-01-01");
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    $join5
where
    r_name = 'AFRICA'
    and CAST(o_orderdate AS Timestamp) >= $border
    and CAST(o_orderdate AS Timestamp) < ($border + Interval("P365D"))
group by
    n_name
order by
    revenue desc;
-- end query
-- TPC-H/TPC-R Forecasting Revenue Change Query (Q6)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$border = Date("1995-01-01");

select
    sum(l_extendedprice * l_discount) as revenue
from
    `{path}lineitem`
where
    CAST(l_shipdate AS Timestamp) >= $border
    and cast(l_shipdate as Timestamp) < ($border + Interval("P365D"))
    and l_discount between 0.07 - 0.01 and 0.07 + 0.01
    and l_quantity < 25;
-- end query
-- TPC-H/TPC-R Volume Shipping Query (Q7)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$join1 = (
select
    l.l_extendedprice * (1 - l.l_discount) as volume,
    DateTime::GetYear(cast(l.l_shipdate as timestamp)) as l_year,
    l.l_orderkey as l_orderkey,
    s.s_nationkey as s_nationkey
from
    `{path}supplier` as s
join
    `{path}lineitem` as l
on
    s.s_suppkey = l.l_suppkey
where cast(cast(l.l_shipdate as Timestamp) as Date) between
    Date('1995-01-01')
    and Date('1996-12-31')
);
$join2 = (
select
    j.volume as volume,
    j.l_year as l_year,
    j.s_nationkey as s_nationkey,
    o.o_orderkey as o_orderkey,
    o.o_custkey as o_custkey
from
    $join1 as j
join
    `{path}orders` as o
on
    o.o_orderkey = j.l_orderkey
);

$join3 = (
select
    j.volume as volume,
    j.l_year as l_year,
    j.s_nationkey as s_nationkey,
    c.c_nationkey as c_nationkey
from
    $join2 as j
join
    `{path}customer` as c
on
    c.c_custkey = j.o_custkey
);

$join4 = (
select
    j.volume as volume,
    j.l_year as l_year,
    j.c_nationkey as c_nationkey,
    j.s_nationkey as s_nationkey,
    n.n_name as n_name
from
    $join3 as j
join
    `{path}nation` as n
on
    j.s_nationkey = n.n_nationkey
);
$join5 = (
select
    j.volume as volume,
    j.l_year as l_year,
    n.n_name as cust_nation,
    j.n_name as supp_nation
from
    $join4 as j
join
    `{path}nation` as n
on
    j.c_nationkey = n.n_nationkey
where (
    (n.n_name = 'PERU' and j.n_name = 'MOZAMBIQUE')
    or (n.n_name = 'MOZAMBIQUE' and j.n_name = 'PERU')
)
);

select
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) as revenue
from
    $join5 as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year;
-- end query
-- TPC-H/TPC-R National Market Share Query (Q8)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$join1 = (
select
    l.l_extendedprice * (1 - l.l_discount) as volume,
    l.l_suppkey as l_suppkey,
    l.l_orderkey as l_orderkey
from
    `{path}part` as p
join
    `{path}lineitem` as l
on
    p.p_partkey = l.l_partkey
where
    p.p_type = 'ECONOMY PLATED COPPER'
);
$join2 = (
select
    j.volume as volume,
    j.l_orderkey as l_orderkey,
    s.s_nationkey as s_nationkey
from
    $join1 as j
join
    `{path}supplier` as s
on
    s.s_suppkey = j.l_suppkey
);
$join3 = (
select
    j.volume as volume,
    j.l_orderkey as l_orderkey,
    n.n_name as nation
from
    $join2 as j
join
    `{path}nation` as n
on
    n.n_nationkey = j.s_nationkey
);
$join4 = (
select
    j.volume as volume,
    j.nation as nation,
    DateTime::GetYear(cast(o.o_orderdate as Timestamp)) as o_year,
    o.o_custkey as o_custkey
from
    $join3 as j
join
    `{path}orders` as o
on
    o.o_orderkey = j.l_orderkey
where cast(cast(o_orderdate as Timestamp) as Date) between Date('1995-01-01') and Date('1996-12-31')
);
$join5 = (
select
    j.volume as volume,
    j.nation as nation,
    j.o_year as o_year,
    c.c_nationkey as c_nationkey
from
    $join4 as j
join
    `{path}customer` as c
on
    c.c_custkey = j.o_custkey
);
$join6 = (
select
    j.volume as volume,
    j.nation as nation,
    j.o_year as o_year,
    n.n_regionkey as n_regionkey
from
    $join5 as j
join
    `{path}nation` as n
on
    n.n_nationkey = j.c_nationkey
);
$join7 = (
select
    j.volume as volume,
    j.nation as nation,
    j.o_year as o_year
from
    $join6 as j
join
    `{path}region` as r
on
    r.r_regionkey = j.n_regionkey
where
    r.r_name = 'AFRICA'
);

select
    o_year,
    sum(case
        when nation = 'MOZAMBIQUE' then volume
        else 0
    end) / sum(volume) as mkt_share
from
    $join7 as all_nations
group by
    o_year
order by
    o_year;
-- end query
-- TPC-H/TPC-R Product Type Profit Measure Query (Q9)
-- Approved February 1998
-- using 1680793381 as a seed to the RNG

$p = (select p_partkey, p_name
from
    `{path}part`
where FIND(p_name, 'rose') IS NOT NULL);

$j1 = (select ps_partkey, ps_suppkey, ps_supplycost
from
    `{path}partsupp` as ps
join $p as p
on ps.ps_partkey = p.p_partkey);

$j2 = (select l_suppkey, l_partkey, l_orderkey, l_extendedprice, l_discount, ps_supplycost, l_quantity
from
    `{path}lineitem` as l
join $j1 as j
on l.l_suppkey = j.ps_suppkey AND l.l_partkey = j.ps_partkey);

$j3 = (select l_orderkey, s_nationkey, l_extendedprice, l_discount, ps_supplycost, l_quantity
from
    `{path}supplier` as s
join $j2 as j
on j.l_suppkey = s.s_suppkey);

$j4 = (select o_orderdate, l_extendedprice, l_discount, ps_supplycost, l_quantity, s_nationkey
from
    `{path}orders` as o
join $j3 as j
on o.o_orderkey = j.l_orderkey);

$j5 = (select n_name, o_orderdate, l_extendedprice, l_discount, ps_supplycost, l_quantity
from
    `{path}nation` as n
join $j4 as j
on j.s_nationkey = n.n_nationkey
);

$profit = (select
    n_name as nation,
    DateTime::GetYear(cast(o_orderdate as timestamp)) as o_year,
    l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
from $j5);

select
    nation,
    o_year,
    sum(amount) as sum_profit
from $profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc;

-- end query
-- TPC-H/TPC-R Returned Item Reporting Query (Q10)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$border = Date("1993-12-01");
$join1 = (
select
    c.c_custkey as c_custkey,
    c.c_name as c_name,
    c.c_acctbal as c_acctbal,
    c.c_address as c_address,
    c.c_phone as c_phone,
    c.c_comment as c_comment,
    c.c_nationkey as c_nationkey,
    o.o_orderkey as o_orderkey
from
    `{path}customer` as c
join
    `{path}orders` as o
on
    c.c_custkey = o.o_custkey
where
    cast(o.o_orderdate as timestamp) >= $border and
    cast(o.o_orderdate as timestamp) < ($border + Interval("P90D"))
);
$join2 = (
select
    j.c_custkey as c_custkey,
    j.c_name as c_name,
    j.c_acctbal as c_acctbal,
    j.c_address as c_address,
    j.c_phone as c_phone,
    j.c_comment as c_comment,
    j.c_nationkey as c_nationkey,
    l.l_extendedprice as l_extendedprice,
    l.l_discount as l_discount
from
    $join1 as j
join
    `{path}lineitem` as l
on
    l.l_orderkey = j.o_orderkey
where
    l.l_returnflag = 'R'
);
$join3 = (
select
    j.c_custkey as c_custkey,
    j.c_name as c_name,
    j.c_acctbal as c_acctbal,
    j.c_address as c_address,
    j.c_phone as c_phone,
    j.c_comment as c_comment,
    j.c_nationkey as c_nationkey,
    j.l_extendedprice as l_extendedprice,
    j.l_discount as l_discount,
    n.n_name as n_name
from
    $join2 as j
join
    `{path}nation` as n
on
    n.n_nationkey = j.c_nationkey
);
select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    $join3
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc
limit 20;
-- end query
-- TPC-H/TPC-R Important Stock Identification Query (Q11)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$join1 = (
select
    ps.ps_partkey as ps_partkey,
    ps.ps_supplycost as ps_supplycost,
    ps.ps_availqty as ps_availqty,
    s.s_nationkey as s_nationkey
from
    `{path}partsupp` as ps
join
    `{path}supplier` as s
on
    ps.ps_suppkey = s.s_suppkey
);
$join2 = (
select
    j.ps_partkey as ps_partkey,
    j.ps_supplycost as ps_supplycost,
    j.ps_availqty as ps_availqty,
    j.s_nationkey as s_nationkey
from
    $join1 as j
join
    `{path}nation` as n
on
    n.n_nationkey = j.s_nationkey
where
    n.n_name = 'CANADA'
);
$threshold = (
select
    sum(ps_supplycost * ps_availqty) * 0.0001000000 as threshold
from
    $join2
);
$values = (
select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    $join2
group by
    ps_partkey
);

select
    v.ps_partkey as ps_partkey,
    v.value as value
from
    $values as v
cross join
    $threshold as t
where
    v.value > t.threshold
order by
    value desc;
-- end query
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
        `{path}orders` as o
        join `{path}lineitem` as l
        on o.o_orderkey == l.l_orderkey
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
    and cast(l_receiptdate as timestamp) >= $border
    and cast(l_receiptdate as timestamp) < ($border + Interval("P365D"))
group by
    l_shipmode
order by
    l_shipmode;
-- end query
-- TPC-H/TPC-R Customer Distribution Query (Q13)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$orders = (
    select
        o_orderkey,
        o_custkey
    from
        `{path}orders`
    where
        o_comment NOT LIKE "%unusual%requests%"
);
select
    c_count as c_count,
    count(*) as custdist
from
    (
        select
            c.c_custkey as c_custkey,
            count(o.o_orderkey) as c_count
        from
            `{path}customer` as c left outer join $orders as o on
                c.c_custkey = o.o_custkey
        group by
            c.c_custkey
    ) as c_orders
group by
    c_count
order by
    custdist desc,
    c_count desc;
-- end query
-- TPC-H/TPC-R Promotion Effect Query (Q14)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$border = Date("1994-08-01");
select
    100.00 * sum(case
        when StartsWith(p.p_type, 'PROMO')
            then l.l_extendedprice * (1 - l.l_discount)
        else 0
    end) / sum(l.l_extendedprice * (1 - l.l_discount)) as promo_revenue
from
    `{path}lineitem` as l
join
    `{path}part` as p
on
    l.l_partkey = p.p_partkey
where
    cast(l.l_shipdate as timestamp) >= $border
    and cast(l.l_shipdate as timestamp) < ($border + Interval("P31D"));
-- end query
-- TPC-H/TPC-R Top Supplier Query (Q15)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$border = Date("1997-03-01");
$revenue0 = (
    select
        l_suppkey as supplier_no,
        sum(l_extendedprice * (1 - l_discount)) as total_revenue,
        cast(sum(l_extendedprice * (1 - l_discount)) as Uint64) as total_revenue_approx
    from
        `{path}lineitem`
    where
        cast(l_shipdate as timestamp) >= $border
        and cast(l_shipdate as timestamp) < ($border + Interval("P92D"))
    group by
        l_suppkey
);
$max_revenue = (
select
    max(total_revenue_approx) as max_revenue
from
    $revenue0
);
$join1 = (
select
    s.s_suppkey as s_suppkey,
    s.s_name as s_name,
    s.s_address as s_address,
    s.s_phone as s_phone,
    r.total_revenue as total_revenue,
    r.total_revenue_approx as total_revenue_approx
from
    `{path}supplier` as s
join
    $revenue0 as r
on
    s.s_suppkey = r.supplier_no
);

select
    j.s_suppkey as s_suppkey,
    j.s_name as s_name,
    j.s_address as s_address,
    j.s_phone as s_phone,
    j.total_revenue as total_revenue
from
    $join1 as j
join
    $max_revenue as m
on
    j.total_revenue_approx = m.max_revenue
order by
    s_suppkey;

-- end query
-- TPC-H/TPC-R Parts/Supplier Relationship Query (Q16)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$join = (
select
    ps.ps_suppkey as ps_suppkey,
    ps.ps_partkey as ps_partkey
from
    `{path}partsupp` as ps
left join
    `{path}supplier` as w
on
    w.s_suppkey = ps.ps_suppkey
where not (s_comment like "%Customer%Complaints%")
);

select
    p.p_brand as p_brand,
    p.p_type as p_type,
    p.p_size as p_size,
    count(distinct j.ps_suppkey) as supplier_cnt
from
    $join as j
join
    `{path}part` as p
on
    p.p_partkey = j.ps_partkey
where
    p.p_brand <> 'Brand#33'
    and (not StartsWith(p.p_type, 'PROMO POLISHED'))
    and (p.p_size = 20 or p.p_size = 27 or p.p_size = 11 or p.p_size = 45 or p.p_size = 40 or p.p_size = 41 or p.p_size = 34 or p.p_size = 36)
group by
    p.p_brand,
    p.p_type,
    p.p_size
order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size
;

-- end query
-- TPC-H/TPC-R Small-Quantity-Order Revenue Query (Q17)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$threshold = (
select
    0.2 * avg(l_quantity) as threshold,
    l.l_partkey as l_partkey
from
    `{path}lineitem` as l
join
    `{path}part` as p
on
    p.p_partkey = l.l_partkey
where
    p.p_brand = 'Brand#35'
    and p.p_container = 'LG DRUM'
group by
    l.l_partkey
);

select
    sum(l.l_extendedprice) / 7.0 as avg_yearly
from
    `{path}lineitem` as l
join
    $threshold as t
on
    t.l_partkey = l.l_partkey
where
    l.l_quantity < t.threshold;
-- end query
-- TPC-H/TPC-R Large Volume Customer Query (Q18)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$in = (
select
    l_orderkey,
    sum(l_quantity) as sum_l_quantity
from
    `{path}lineitem`
group by
    l_orderkey having
        sum(l_quantity) > 315
);

$join1 = (
select
    c.c_name as c_name,
    c.c_custkey as c_custkey,
    o.o_orderkey as o_orderkey,
    o.o_orderdate as o_orderdate,
    o.o_totalprice as o_totalprice
from
    `{path}customer` as c
join
    `{path}orders` as o
on
    c.c_custkey = o.o_custkey
);
select
    j.c_name as c_name,
    j.c_custkey as c_custkey,
    j.o_orderkey as o_orderkey,
    j.o_orderdate as o_orderdate,
    j.o_totalprice as o_totalprice,
    sum(i.sum_l_quantity) as sum_l_quantity
from
    $join1 as j
join
    $in as i
on
    i.l_orderkey = j.o_orderkey
group by
    j.c_name,
    j.c_custkey,
    j.o_orderkey,
    j.o_orderdate,
    j.o_totalprice
order by
    o_totalprice desc,
    o_orderdate
limit 100;
-- end query
-- TPC-H/TPC-R Discounted Revenue Query (Q19)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

select
    sum(l.l_extendedprice* (1 - l.l_discount)) as revenue
from
    `{path}lineitem` as l
join
    `{path}part` as p
on
    p.p_partkey = l.l_partkey
where
    (
        p.p_brand = 'Brand#23'
        and (p.p_container = 'SM CASE' or p.p_container = 'SM BOX' or p.p_container = 'SM PACK' or p.p_container = 'SM PKG')
        and l.l_quantity >= 7 and l.l_quantity <= 7 + 10
        and p.p_size between 1 and 5
        and (l.l_shipmode = 'AIR' or l.l_shipmode = 'AIR REG')
        and l.l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p.p_brand = 'Brand#15'
        and (p.p_container = 'MED BAG' or p.p_container = 'MED BOX' or p.p_container = 'MED PKG' or p.p_container = 'MED PACK')
        and l.l_quantity >= 17 and l.l_quantity <= 17 + 10
        and p.p_size between 1 and 10
        and (l.l_shipmode = 'AIR' or l.l_shipmode = 'AIR REG')
        and l.l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p.p_brand = 'Brand#44'
        and (p.p_container = 'LG CASE' or p.p_container = 'LG BOX' or p.p_container = 'LG PACK' or p.p_container = 'LG PKG')
        and l.l_quantity >= 25 and l.l_quantity <= 25 + 10
        and p.p_size between 1 and 15
        and (l.l_shipmode = 'AIR' or l.l_shipmode = 'AIR REG')
        and l.l_shipinstruct = 'DELIVER IN PERSON'
    );
-- end query
-- TPC-H/TPC-R Potential Part Promotion Query (Q20)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$border = Date("1993-01-01");
$threshold = (
select
    0.5 * sum(l_quantity) as threshold,
    l_partkey as l_partkey,
    l_suppkey as l_suppkey
from
    `{path}lineitem`
where
    cast(l_shipdate as timestamp) >= $border
    and cast(l_shipdate as timestamp) < ($border + Interval("P365D"))
group by
    l_partkey, l_suppkey
);

$parts = (
select
    p_partkey
from
    `{path}part`
where
    StartsWith(p_name, 'maroon')
);

$join1 = (
select
    ps.ps_suppkey as ps_suppkey,
    ps.ps_availqty as ps_availqty,
    ps.ps_partkey as ps_partkey
from
    `{path}partsupp` as ps
join any
    $parts as p
on
    ps.ps_partkey = p.p_partkey
);

$join2 = (
select
    distinct(j.ps_suppkey) as ps_suppkey
from
    $join1 as j
join any
    $threshold as t
on
    j.ps_partkey = t.l_partkey and j.ps_suppkey = t.l_suppkey
where
    j.ps_availqty > t.threshold
);

$join3 = (
select
    j.ps_suppkey as ps_suppkey,
    s.s_name as s_name,
    s.s_address as s_address,
    s.s_nationkey as s_nationkey
from
    $join2 as j
join any
    `{path}supplier` as s
on
    j.ps_suppkey = s.s_suppkey
);

select
    j.s_name as s_name,
    j.s_address as s_address
from
    $join3 as j
join
    `{path}nation` as n
on
    j.s_nationkey = n.n_nationkey
where
    n.n_name = 'VIETNAM'
order by
    s_name;

-- end query
-- TPC-H/TPC-R Suppliers Who Kept Orders Waiting Query (Q21)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$n = select n_nationkey from `{path}nation`
where n_name = 'EGYPT';

$s = select s_name, s_suppkey from `{path}supplier` as supplier
join $n as nation
on supplier.s_nationkey = nation.n_nationkey;

$l = select l_suppkey, l_orderkey from `{path}lineitem`
where l_receiptdate > l_commitdate;

$j1 = select s_name, l_suppkey, l_orderkey from $l as l1
join $s as supplier
on l1.l_suppkey = supplier.s_suppkey;

-- exists
$j2 = select l1.l_orderkey as l_orderkey, l1.l_suppkey as l_suppkey, l1.s_name as s_name, l2.l_receiptdate as l_receiptdate, l2.l_commitdate as l_commitdate from $j1 as l1
join `{path}lineitem` as l2
on l1.l_orderkey = l2.l_orderkey
where l2.l_suppkey <> l1.l_suppkey;

$j2_1 = select s_name, l1.l_suppkey as l_suppkey, l1.l_orderkey as l_orderkey from $j1 as l1
left semi join $j2 as l2
on l1.l_orderkey = l2.l_orderkey;

-- not exists
$j2_2 = select l_orderkey from $j2 where l_receiptdate > l_commitdate;

$j3 = select s_name, l_suppkey, l_orderkey from $j2_1 as l1
left only join $j2_2 as l3
on l1.l_orderkey = l3.l_orderkey;

$j4 = select s_name from $j3 as l1
join `{path}orders` as orders
on orders.o_orderkey = l1.l_orderkey
where o_orderstatus = 'F';

select s_name,
    count(*) as numwait from $j4
group by
    s_name
order by
    numwait desc,
    s_name
limit 100;
-- end query
-- TPC-H/TPC-R Global Sales Opportunity Query (Q22)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$customers = (
select
    c_acctbal,
    c_custkey,
    Substring(c_phone, 0u, 2u) as cntrycode
from
    `{path}customer`
where (Substring(c_phone, 0u, 2u) = '31' or Substring(c_phone, 0u, 2u) = '29' or Substring(c_phone, 0u, 2u) = '30' or Substring(c_phone, 0u, 2u) = '26' or Substring(c_phone, 0u, 2u) = '28' or Substring(c_phone, 0u, 2u) = '25' or Substring(c_phone, 0u, 2u) = '15')
);
$avg = (
select
    avg(c_acctbal) as a
from
    $customers
where
    c_acctbal > 0.00
);
$join1 = (
select
    c.c_acctbal as c_acctbal,
    c.c_custkey as c_custkey,
    c.cntrycode as cntrycode
from
    $customers as c
cross join
    $avg as a
where
    c.c_acctbal > a.a
);
$join2 = (
select
    j.cntrycode as cntrycode,
    c_custkey,
    j.c_acctbal as c_acctbal
from
    $join1 as j
left only join
    `{path}orders` as o
on
    o.o_custkey = j.c_custkey
);

select
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
from
    $join2 as custsale
group by
    cntrycode
order by
    cntrycode;

-- end query
