{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query42.tpl and seed 1819994127
select  dt.d_year
 	,item.i_category_id
 	,item.i_category
 	,sum(ss_ext_sales_price) sum_price
 from 	{{date_dim}} dt
 	cross join {{store_sales}} as store_sales
 	cross join {{item}} as item
 where dt.d_date_sk = store_sales.ss_sold_date_sk
 	and store_sales.ss_item_sk = item.i_item_sk
 	and item.i_manager_id = 1
 	and dt.d_moy=11
 	and dt.d_year=2000
 group by 	dt.d_year
 		,item.i_category_id
 		,item.i_category
 order by       sum_price desc,dt.d_year
 		,item.i_category_id
 		,item.i_category
limit 100 ;

-- end query 1 in stream 0 using template query42.tpl
