pragma TablePathPrefix = "/Root/test/ds/";

-- NB: Subquerys
-- start query 1 in stream 0 using template query61.tpl and seed 1930872976
select  promotions,total,cast(promotions as float)/cast(total as float)*100
from
  (select sum(ss_ext_sales_price) promotions
   from  store_sales
        cross join store
        cross join promotion
        cross join date_dim
        cross join customer
        cross join customer_address
        cross join item
   where ss_sold_date_sk = d_date_sk
   and   ss_store_sk = s_store_sk
   and   ss_promo_sk = p_promo_sk
   and   ss_customer_sk= c_customer_sk
   and   ca_address_sk = c_current_addr_sk
   and   ss_item_sk = i_item_sk
   and   ca_gmt_offset = -6
   and   i_category = 'Sports'
   and   (p_channel_dmail = 'Y' or p_channel_email = 'Y' or p_channel_tv = 'Y')
   and   s_gmt_offset = -6
   and   d_year = 2001
   and   d_moy  = 12) promotional_sales cross join
  (select sum(ss_ext_sales_price) total
   from  store_sales
        cross join store
        cross join date_dim
        cross join customer
        cross join customer_address
        cross join item
   where ss_sold_date_sk = d_date_sk
   and   ss_store_sk = s_store_sk
   and   ss_customer_sk= c_customer_sk
   and   ca_address_sk = c_current_addr_sk
   and   ss_item_sk = i_item_sk
   and   ca_gmt_offset = -6
   and   i_category = 'Sports'
   and   s_gmt_offset = -6
   and   d_year = 2001
   and   d_moy  = 12) all_sales
order by promotions, total
limit 100;
