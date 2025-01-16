{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query29.tpl and seed 2031708268
select
     item.i_item_id
    ,item.i_item_desc
    ,store.s_store_id
    ,store.s_store_name
    ,sum(ss_quantity)        as store_sales_quantity
    ,sum(sr_return_quantity) as store_returns_quantity
    ,sum(cs_quantity)        as catalog_sales_quantity
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
     d1.d_moy               = 9
 and d1.d_year              = 1999
 and d1.d_date_sk           = ss_sold_date_sk
 and i_item_sk              = ss_item_sk
 and s_store_sk             = ss_store_sk
 and ss_customer_sk         = sr_customer_sk
 and ss_item_sk             = sr_item_sk
 and ss_ticket_number       = sr_ticket_number
 and sr_returned_date_sk    = d2.d_date_sk
 and d2.d_moy               between 9 and  9 + 3
 and d2.d_year              = 1999
 and sr_customer_sk         = cs_bill_customer_sk
 and sr_item_sk             = cs_item_sk
 and cs_sold_date_sk        = d3.d_date_sk
 and d3.d_year              in (1999,1999+1,1999+2)
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
