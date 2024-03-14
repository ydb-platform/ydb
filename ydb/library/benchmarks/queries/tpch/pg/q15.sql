{% include 'header.sql.jinja' %}

-- TPC-H/TPC-R Top Supplier Query (Q15)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG

create view revenue0 (supplier_no, total_revenue) as
    select
        l_suppkey,
        sum(l_extendedprice * (1 - l_discount))
    from
        {{lineitem}}
    where
        l_shipdate >= date '1997-03-01'
        and l_shipdate < date '1997-03-01' + interval '3' month
    group by
        l_suppkey;

select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    {{supplier}},
    revenue0
where
    s_suppkey = supplier_no
    and total_revenue = (
        select
            max(total_revenue)
        from
            revenue0
    )
order by
    s_suppkey;

drop view revenue0;

