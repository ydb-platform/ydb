{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query12.tpl and seed 345591136
select item.i_item_id
      ,item.i_item_desc
      ,item.i_category
      ,item.i_class
      ,item.i_current_price
      ,sum(web_sales.ws_ext_sales_price) as itemrevenue
      ,sum(web_sales.ws_ext_sales_price)*100/sum(sum(web_sales.ws_ext_sales_price)) over
          (partition by item.i_class) as revenueratio
from
	{{web_sales}} as web_sales
        cross join {{item}} as item
    	cross join {{date_dim}} as date_dim
where
	web_sales.ws_item_sk = item.i_item_sk
  	and item.i_category in ('Sports', 'Books', 'Home')
  	and web_sales.ws_sold_date_sk = date_dim.d_date_sk
	and cast(date_dim.d_date as date) between cast('1999-02-22' as date)
				and (cast('1999-02-22' as date) + cast('P30D' as interval))
group by
	item.i_item_id
        ,item.i_item_desc
        ,item.i_category
        ,item.i_class
        ,item.i_current_price
order by
	item.i_category
        ,item.i_class
        ,item.i_item_id
        ,item.i_item_desc
        ,revenueratio
limit 100;
-- end query 1 in stream 0 using template query12.tpl
