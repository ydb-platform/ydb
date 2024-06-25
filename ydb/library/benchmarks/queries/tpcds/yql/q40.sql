{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query40.tpl and seed 1819994127
select
   warehouse.w_state
  ,item.i_item_id
  ,sum(case when (cast(d_date as date) < cast ('2000-03-18' as date))
 		then cs_sales_price - coalesce(cr_refunded_cash,$z0) else $z0 end) as sales_before
  ,sum(case when (cast(d_date as date) >= cast ('2000-03-18' as date))
 		then cs_sales_price - coalesce(cr_refunded_cash,$z0) else $z0 end) as sales_after
 from
   {{catalog_sales}} as catalog_sales
   left join {{catalog_returns}} as catalog_returns on
       (catalog_sales.cs_order_number = catalog_returns.cr_order_number
        and catalog_sales.cs_item_sk = catalog_returns.cr_item_sk)
  cross join {{warehouse}} as warehouse
  cross join {{item}} as item
  cross join {{date_dim}} as date_dim
 where
     i_current_price between $z0_99 and $z1_49
 and i_item_sk          = cs_item_sk
 and cs_warehouse_sk    = w_warehouse_sk
 and cs_sold_date_sk    = d_date_sk
 and cast(d_date as date) between (cast ('2000-03-18' as date) - DateTime::IntervalFromDays(30))
                and (cast ('2000-03-18' as date) + DateTime::IntervalFromDays(30))
 group by
    warehouse.w_state,item.i_item_id
 order by warehouse.w_state,item.i_item_id
limit 100;

-- end query 1 in stream 0 using template query40.tpl
