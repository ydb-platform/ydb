--!syntax_pg
--TPC-DS Q61

-- start query 1 in stream 0 using template ../query_templates/query61.tpl
select  promotions,total,cast(promotions as decimal(15,4))/cast(total as decimal(15,4))*100::numeric
from
  (select sum(ss_ext_sales_price) promotions
   from  plato.store_sales
        ,plato.store
        ,plato.promotion
        ,plato.date_dim
        ,plato.customer
        ,plato.customer_address 
        ,plato.item
   where ss_sold_date_sk = d_date_sk
   and   ss_store_sk = s_store_sk
   and   ss_promo_sk = p_promo_sk
   and   ss_customer_sk= c_customer_sk
   and   ca_address_sk = c_current_addr_sk
   and   ss_item_sk = i_item_sk 
   and   ca_gmt_offset = -7::numeric
   and   i_category = 'Books'
   and   (p_channel_dmail = 'Y' or p_channel_email = 'Y' or p_channel_tv = 'Y')
   and   s_gmt_offset = -7::numeric
   and   d_year = 1999
   and   d_moy  = 11) promotional_sales,
  (select sum(ss_ext_sales_price) total
   from  plato.store_sales
        ,plato.store
        ,plato.date_dim
        ,plato.customer
        ,plato.customer_address
        ,plato.item
   where ss_sold_date_sk = d_date_sk
   and   ss_store_sk = s_store_sk
   and   ss_customer_sk= c_customer_sk
   and   ca_address_sk = c_current_addr_sk
   and   ss_item_sk = i_item_sk
   and   ca_gmt_offset = -7::numeric
   and   i_category = 'Books'
   and   s_gmt_offset = -7::numeric
   and   d_year = 1999
   and   d_moy  = 11) all_sales
order by promotions, total
limit 100;

-- end query 1 in stream 0 using template ../query_templates/query61.tpl
