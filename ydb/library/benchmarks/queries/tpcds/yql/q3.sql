{% include 'header.sql.jinja' %}

-- NB: Subquerys
$todecimal = ($x) -> {
  return cast(cast($x as string?) as decimal(7,2))
};

-- start query 1 in stream 0 using template query3.tpl and seed 2031708268
select  dt.d_year
       ,item.i_brand_id brand_id
       ,item.i_brand brand
       ,sum($todecimal(ss_sales_price)) sum_agg
 from  {{date_dim}} dt
      cross join {{store_sales}} as store_sales
      cross join {{item}} as item
 where dt.d_date_sk = store_sales.ss_sold_date_sk
   and store_sales.ss_item_sk = item.i_item_sk
   and item.i_manufact_id = 816
   and dt.d_moy=11
 group by dt.d_year
      ,item.i_brand
      ,item.i_brand_id
 order by dt.d_year
         ,sum_agg desc
         ,brand_id
 limit 100;

-- end query 1 in stream 0 using template query3.tpl
