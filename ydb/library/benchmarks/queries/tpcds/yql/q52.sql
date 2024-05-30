{% include 'header.sql.jinja' %}

-- start query 1 in stream 0 using template query52.tpl and seed 1819994127
$todecimal = ($x) -> {
  return cast(cast($x as string?) as decimal(7,2))
};

select  dt.d_year
 	,item.i_brand_id brand_id
 	,item.i_brand brand
 	,sum($todecimal(ss_ext_sales_price)) ext_price
 from {{date_dim}} dt
 cross join
      {{store_sales}} as store_sales
 cross join
      {{item}} as item
 where dt.d_date_sk = store_sales.ss_sold_date_sk
    and store_sales.ss_item_sk = item.i_item_sk
    and item.i_manager_id = 1
    and dt.d_moy=12
    and dt.d_year=2000
 group by dt.d_year
 	,item.i_brand
 	,item.i_brand_id
 order by dt.d_year
 	,ext_price desc
 	,brand_id
limit 100 ;

-- end query 1 in stream 0 using template query52.tpl
