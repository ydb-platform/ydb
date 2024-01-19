{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query37.tpl and seed 301843662
select  item.i_item_id
       ,item.i_item_desc
       ,item.i_current_price
 from {{item}} as item
 cross join {{inventory}} as inventory
 cross join {{date_dim}} as date_dim
 cross join {{catalog_sales}} as catalog_sales
 where i_current_price between 39 and 39 + 30
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and cast(d_date as date) between cast('2001-01-16' as date) and (cast('2001-01-16' as date) + DateTime::IntervalFromDays(60))
 and i_manufact_id in (765,886,889,728)
 and inv_quantity_on_hand between 100 and 500
 and cs_item_sk = i_item_sk
 group by item.i_item_id,item.i_item_desc,item.i_current_price
 order by item.i_item_id
 limit 100;

-- end query 1 in stream 0 using template query37.tpl
