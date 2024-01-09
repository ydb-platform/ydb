{% include 'header.sql.jinja' %}

-- start query 1 in stream 0 using template query50.tpl and seed 1819994127
select
   store.s_store_name
  ,store.s_company_id
  ,store.s_street_number
  ,store.s_street_name
  ,store.s_street_type
  ,store.s_suite_number
  ,store.s_city
  ,store.s_county
  ,store.s_state
  ,store.s_zip
  ,sum(case when (store_returns.sr_returned_date_sk - store_sales.ss_sold_date_sk <= 30 ) then 1 else 0 end)  as `30 days`
  ,sum(case when (store_returns.sr_returned_date_sk - store_sales.ss_sold_date_sk > 30) and
                 (store_returns.sr_returned_date_sk - store_sales.ss_sold_date_sk <= 60) then 1 else 0 end )  as `31-60 days`
  ,sum(case when (store_returns.sr_returned_date_sk - store_sales.ss_sold_date_sk > 60) and
                 (store_returns.sr_returned_date_sk - store_sales.ss_sold_date_sk <= 90) then 1 else 0 end)  as `61-90 days`
  ,sum(case when (store_returns.sr_returned_date_sk - store_sales.ss_sold_date_sk > 90) and
                 (store_returns.sr_returned_date_sk - store_sales.ss_sold_date_sk <= 120) then 1 else 0 end)  as `91-120 days`
  ,sum(case when (store_returns.sr_returned_date_sk - store_sales.ss_sold_date_sk  > 120) then 1 else 0 end)  as `>120 days`
from
   {{store_sales}} as store_sales
cross join
   {{store_returns}} as store_returns
cross join
   {{store}} as store
cross join
   {{date_dim}} as d1
cross join
   {{date_dim}} as d2
where
    d2.d_year = 1998
and d2.d_moy  = 9
and ss_ticket_number = sr_ticket_number
and ss_item_sk = sr_item_sk
and ss_sold_date_sk   = d1.d_date_sk
and sr_returned_date_sk   = d2.d_date_sk
and ss_customer_sk = sr_customer_sk
and ss_store_sk = s_store_sk
group by
   store.s_store_name
  ,store.s_company_id
  ,store.s_street_number
  ,store.s_street_name
  ,store.s_street_type
  ,store.s_suite_number
  ,store.s_city
  ,store.s_county
  ,store.s_state
  ,store.s_zip
order by s_store_name
        ,s_company_id
        ,s_street_number
        ,s_street_name
        ,s_street_type
        ,s_suite_number
        ,s_city
        ,s_county
        ,s_state
        ,s_zip
limit 100;

-- end query 1 in stream 0 using template query50.tpl
;
