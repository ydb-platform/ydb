{% include 'header.sql.jinja' %}

-- NB: Subquerys
$bla1 = (select distinct
       COALESCE(c_last_name,'') as c_last_name,
       COALESCE(c_first_name,'') as c_first_name,
       COALESCE(cast(d_date as date), cast(0 as Date)) as d_date
       from {{store_sales}} as store_sales
       cross join {{date_dim}} as date_dim
       cross join  {{customer}} as customer
       where store_sales.ss_sold_date_sk = date_dim.d_date_sk
         and store_sales.ss_customer_sk = customer.c_customer_sk
         and d_month_seq between 1221 and 1221+11);

$bla2 = ((select distinct
       COALESCE(c_last_name,'') as c_last_name,
       COALESCE(c_first_name,'') as c_first_name,
       COALESCE(cast(d_date as date), cast(0 as Date)) as d_date
       from {{catalog_sales}} as catalog_sales
       cross join {{date_dim}} as date_dim
       cross join {{customer}} as customer
       where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
         and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1221 and 1221+11)
       union all
      (select distinct
       COALESCE(c_last_name,'') as c_last_name,
       COALESCE(c_first_name,'') as c_first_name,
       COALESCE(cast(d_date as date), cast(0 as Date)) as d_date
       from {{web_sales}} as web_sales
       cross join {{date_dim}} as date_dim
       cross join {{customer}} as customer
       where web_sales.ws_sold_date_sk = date_dim.d_date_sk
         and web_sales.ws_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1221 and 1221+11));

-- start query 1 in stream 0 using template query87.tpl and seed 1819994127
select count(*)
from $bla1 bla1 left only join $bla2 bla2 using (c_last_name, c_first_name, d_date)
;

-- end query 1 in stream 0 using template query87.tpl
