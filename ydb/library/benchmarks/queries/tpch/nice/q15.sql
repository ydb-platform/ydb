{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Top Supplier Query (Q15)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

$revenue0 = (
    select
        l_suppkey as supplier_no,
        sum(l_extendedprice * (1 - l_discount)) as total_revenue
    from
        {{lineitem}}
    where
        l_shipdate >= date('1996-01-01')
        and l_shipdate < date('1996-01-01') + interval('P90D')
    group by
        l_suppkey);
        
select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    {{supplier}}
    cross join $revenue0 as revenu0
    cross join (
        select
            max(total_revenue) as max_total_revenue
        from
            $revenue0) as max_revenue
where
    s_suppkey = supplier_no
    and total_revenue = max_total_revenue
order by
    s_suppkey;
