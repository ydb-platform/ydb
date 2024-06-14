{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query19.tpl and seed 1930872976
select  item.i_brand_id brand_id, item.i_brand brand, item.i_manufact_id, item.i_manufact,
 	sum(ss_ext_sales_price) ext_price
 from {{date_dim}} as date_dim 
 cross join {{store_sales}} as store_sales 
 cross join {{item}} as item
 cross join {{customer}} as customer
 cross join {{customer_address}} as customer_address
 cross join {{store}} as store
 where d_date_sk = ss_sold_date_sk
   and ss_item_sk = i_item_sk
   and i_manager_id=16
   and d_moy=12
   and d_year=1998
   and ss_customer_sk = c_customer_sk
   and c_current_addr_sk = ca_address_sk
   and substring(cast(ca_zip as string),0,5) <> substring(cast(s_zip as string),0,5)
   and ss_store_sk = s_store_sk
 group by item.i_brand
      ,item.i_brand_id
      ,item.i_manufact_id
      ,item.i_manufact
 order by ext_price desc
         ,brand_id
         ,brand
         ,item.i_manufact_id
         ,item.i_manufact
limit 100 ;

-- end query 1 in stream 0 using template query19.tpl
