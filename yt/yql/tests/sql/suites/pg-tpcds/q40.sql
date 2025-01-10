--!syntax_pg
--TPC-DS Q40

-- start query 1 in stream 0 using template ../query_templates/query40.tpl
select  
   w_state
  ,i_item_id
  ,sum(case when (cast(d_date as date) < cast ('1998-04-08' as date)) 
 		then cs_sales_price - coalesce(cr_refunded_cash,0::numeric) else 0::numeric end) as sales_before
  ,sum(case when (cast(d_date as date) >= cast ('1998-04-08' as date)) 
 		then cs_sales_price - coalesce(cr_refunded_cash,0::numeric) else 0::numeric end) as sales_after
 from
   plato.catalog_sales left outer join plato.catalog_returns on
       (cs_order_number = cr_order_number 
        and cs_item_sk = cr_item_sk)
  ,plato.warehouse 
  ,plato.item
  ,plato.date_dim
 where
     i_current_price between 0.99::numeric and 1.49::numeric
 and i_item_sk          = cs_item_sk
 and cs_warehouse_sk    = w_warehouse_sk 
 and cs_sold_date_sk    = d_date_sk
 and d_date between (cast ('1998-04-08' as date) - interval '30' day)::date
                and (cast ('1998-04-08' as date) + interval '30' day)::date
 group by
    w_state,i_item_id
 order by w_state,i_item_id
limit 100;

-- end query 1 in stream 0 using template ../query_templates/query40.tpl
