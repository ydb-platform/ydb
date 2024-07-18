{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query20.tpl and seed 345591136
select  item.i_item_id
       ,item.i_item_desc
       ,item.i_category
       ,item.i_class
       ,item.i_current_price
       ,sum(cs_ext_sales_price) as itemrevenue
       ,$upscale(sum(cs_ext_sales_price)*100)/sum($upscale(sum(cs_ext_sales_price))) over
           (partition by item.i_class) as revenueratio
 from	{{catalog_sales}} as catalog_sales
     cross join {{item}} as item
     cross join {{date_dim}} as date_dim
 where cs_item_sk = i_item_sk
   and i_category in ('Shoes', 'Electronics', 'Children')
   and cs_sold_date_sk = d_date_sk
 and cast(d_date as date) between cast('2001-03-14' as date)
 				and (cast('2001-03-14' as date) + DateTime::IntervalFromDays(30))
 group by item.i_item_id
         ,item.i_item_desc
         ,item.i_category
         ,item.i_class
         ,item.i_current_price
 order by item.i_category
         ,item.i_class
         ,item.i_item_id
         ,item.i_item_desc
         ,revenueratio
limit 100;

-- end query 1 in stream 0 using template query20.tpl
