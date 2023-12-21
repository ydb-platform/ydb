{% include 'header.sql.jinja' %}

-- start query 1 in stream 0 using template query55.tpl and seed 2031708268
select  item.i_brand_id brand_id, item.i_brand brand,
 	sum(ss_ext_sales_price) ext_price
 from {{date_dim}} as date_dim
 cross join {{store_sales}} as store_sales
 cross join {{item}} as item
 where d_date_sk = ss_sold_date_sk
 	and ss_item_sk = i_item_sk
 	and i_manager_id=13
 	and d_moy=11
 	and d_year=1999
 group by item.i_brand, item.i_brand_id
 order by ext_price desc, brand_id
limit 100 ;

-- end query 1 in stream 0 using template query55.tpl
