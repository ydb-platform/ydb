{% include 'header.sql.jinja' %}

select
    w_warehouse_name,
    sm.sm_type,
    cc.cc_name,
    sum(case when (cs.cs_ship_date_sk - cs.cs_sold_date_sk <= 30 ) then 1 else 0 end)  as `30 days`,
    sum(case when (cs.cs_ship_date_sk - cs.cs_sold_date_sk > 30) and
                 (cs.cs_ship_date_sk - cs.cs_sold_date_sk <= 60) then 1 else 0 end )  as `31-60 days`,
    sum(case when (cs.cs_ship_date_sk - cs.cs_sold_date_sk > 60) and
                 (cs.cs_ship_date_sk - cs.cs_sold_date_sk <= 90) then 1 else 0 end)  as `61-90 days`,
    sum(case when (cs.cs_ship_date_sk - cs.cs_sold_date_sk > 90) and
                 (cs.cs_ship_date_sk - cs.cs_sold_date_sk <= 120) then 1 else 0 end)  as `91-120 days`,
    sum(case when (cs.cs_ship_date_sk - cs.cs_sold_date_sk  > 120) then 1 else 0 end)  as `>120 days`
from
    {{catalog_sales}} as cs
    cross join {{warehouse}} as w
    cross join {{ship_mode}} as sm
    cross join {{call_center}} as cc
    cross join {{date_dim}} as d
where
    d.d_month_seq between 1178 and 1178 + 11
    and cs.cs_ship_date_sk   = d.d_date_sk
    and cs.cs_warehouse_sk   = w.w_warehouse_sk
    and cs.cs_ship_mode_sk   = sm.sm_ship_mode_sk
    and cs.cs_call_center_sk = cc.cc_call_center_sk
group by
    substring(cast(w.w_warehouse_name as String), 0, 20) as w_warehouse_name,
    sm.sm_type,
    cc.cc_name
order by w_warehouse_name,
         sm.sm_type,
         cc.cc_name
limit 100;
