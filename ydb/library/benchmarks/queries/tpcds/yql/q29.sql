{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query29.tpl and seed 2031708268
select
     item.i_item_id
    ,item.i_item_desc
    ,store.s_store_id
    ,store.s_store_name
    ,stddev_samp(ss_quantity)        as store_sales_quantity
    ,stddev_samp(sr_return_quantity) as store_returns_quantity
    ,stddev_samp(cs_quantity)        as catalog_sales_quantity
 from
    {{store_sales}} as store_sales
   cross join {{store_returns}} as store_returns
   cross join {{catalog_sales}} as catalog_sales
   cross join {{date_dim}}             d1
   cross join {{date_dim}}             d2
   cross join {{date_dim}}             d3
   cross join {{store}} as store
   cross join {{item}} as item
 where
     d1.d_moy               = 4
 and d1.d_year              = 1998
 and d1.d_date_sk           = ss_sold_date_sk
 and i_item_sk              = ss_item_sk
 and s_store_sk             = ss_store_sk
 and ss_customer_sk         = sr_customer_sk
 and ss_item_sk             = sr_item_sk
 and ss_ticket_number       = sr_ticket_number
 and sr_returned_date_sk    = d2.d_date_sk
 and d2.d_moy               between 4 and  4 + 3
 and d2.d_year              = 1998
 and sr_customer_sk         = cs_bill_customer_sk
 and sr_item_sk             = cs_item_sk
 and cs_sold_date_sk        = d3.d_date_sk
 and d3.d_year              in (1998,1998+1,1998+2)
 group by
    item.i_item_id
   ,item.i_item_desc
   ,store.s_store_id
   ,store.s_store_name
 order by
    item.i_item_id
   ,item.i_item_desc
   ,store.s_store_id
   ,store.s_store_name
 limit 100;

-- end query 1 in stream 0 using template query29.tpl
