{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query62.tpl and seed 1819994127
select
   bla
  ,ship_mode.sm_type
  ,web_site.web_name
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk <= 30 ) then 1 else 0 end)  as `30 days`
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 30) and
                 (ws_ship_date_sk - ws_sold_date_sk <= 60) then 1 else 0 end )  as `31-60 days`
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 60) and
                 (ws_ship_date_sk - ws_sold_date_sk <= 90) then 1 else 0 end)  as `61-90 days`
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 90) and
                 (ws_ship_date_sk - ws_sold_date_sk <= 120) then 1 else 0 end)  as `91-120 days`
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk  > 120) then 1 else 0 end)  as `>120 days`
from
   {{web_sales}} as web_sales
  cross join {{warehouse}} as warehouse
  cross join {{ship_mode}} as ship_mode
  cross join {{web_site}} as web_site
  cross join {{date_dim}} as date_dim
where
    d_month_seq between 1215 and 1215 + 11
and ws_ship_date_sk   = d_date_sk
and ws_warehouse_sk   = w_warehouse_sk
and ws_ship_mode_sk   = sm_ship_mode_sk
and ws_web_site_sk    = web_site_sk
group by
   substring(cast(w_warehouse_name as string),0,20) as bla
  ,ship_mode.sm_type
  ,web_site.web_name
order by bla
        ,ship_mode.sm_type
       ,web_site.web_name
limit 100;

-- end query 1 in stream 0 using template query62.tpl
