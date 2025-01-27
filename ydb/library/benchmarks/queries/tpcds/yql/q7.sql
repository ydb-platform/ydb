{% include 'header.sql.jinja' %}

-- start query 1 in stream 0 using template query7.tpl and seed 1930872976
select  item.i_item_id as i_item_id,
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4
 from {{store_sales}} 
 cross join {{customer_demographics}} as customer_demographics
 cross join {{date_dim}} as date_dim
 cross join {{item}} as item
 cross join {{promotion}} as promotion
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_cdemo_sk = cd_demo_sk and
       ss_promo_sk = p_promo_sk and
       cd_gender = 'M' and
       cd_marital_status = 'S' and
       cd_education_status = 'College' and
       (p_channel_email = 'N' or p_channel_event = 'N') and
       d_year = 2000
 group by item.i_item_id
 order by i_item_id
 limit 100;

-- end query 1 in stream 0 using template query7.tpl
