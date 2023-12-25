{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query26.tpl and seed 1930872976
select  item.i_item_id,
        avg(cs_quantity) agg1,
        avg(cs_list_price) agg2,
        avg(cs_coupon_amt) agg3,
        avg(cs_sales_price) agg4
 from {{catalog_sales}} as catalog_sales
 cross join {{customer_demographics}}  as customer_demographics
 cross join {{date_dim}} as date_dim
 cross join {{item}} as item
 cross join {{promotion}} as promotion
 where cs_sold_date_sk = d_date_sk and
       cs_item_sk = i_item_sk and
       cs_bill_cdemo_sk = cd_demo_sk and
       cs_promo_sk = p_promo_sk and
       cd_gender = 'F' and
       cd_marital_status = 'S' and
       cd_education_status = 'College' and
       (p_channel_email = 'N' or p_channel_event = 'N') and
       d_year = 1998
 group by item.i_item_id
 order by item.i_item_id
 limit 100;

-- end query 1 in stream 0 using template query26.tpl
