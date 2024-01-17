{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query82.tpl and seed 55585014
select  item.i_item_id
       ,item.i_item_desc
       ,item.i_current_price
 from {{item}} as item
 cross join {{inventory}} as inventory
 cross join {{date_dim}} as date_dim
 cross join {{store_sales}} as store_sales
 where i_current_price between 49 and 49+30
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and cast(d_date as date) between cast('2001-01-28' as date) and (cast('2001-01-28' as date) + DateTime::IntervalFromDays(60))
 and i_manufact_id in (80,675,292,17)
 and inv_quantity_on_hand between 100 and 500
 and ss_item_sk = i_item_sk
 group by item.i_item_id,item.i_item_desc,item.i_current_price
 order by item.i_item_id
 limit 100;

-- end query 1 in stream 0 using template query82.tpl
