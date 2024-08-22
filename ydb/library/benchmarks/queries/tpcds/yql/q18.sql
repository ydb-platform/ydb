{% include 'header.sql.jinja' %}

-- TODO this commit should be reverted upon proper fix for https://github.com/ydb-platform/ydb/issues/7565
-- NB: Subquerys
-- start query 1 in stream 0 using template query18.tpl and seed 1978355063
select  item.i_item_id,
        customer_address.ca_country,
        customer_address.ca_state,
        customer_address.ca_county,
        avg( cast(cs_quantity as float)) agg1,
        avg( cast(cs_list_price as float)) agg2,
        avg( cast(cs_coupon_amt as float)) agg3,
        avg( cast(cs_sales_price as float)) agg4,
        avg( cast(cs_net_profit as float)) agg5,
        avg( cast(c_birth_year as float)) agg6,
        avg( cast(cd1.cd_dep_count as float)) agg7
 from {{catalog_sales}} as catalog_sales
 cross join {{customer_demographics}} cd1
 cross join {{date_dim}} as date_dim
 cross join {{customer}} as customer
 cross join {{customer_demographics}} cd2
 cross join {{customer_address}} as customer_address
 cross join {{item}} as item
 where cs_sold_date_sk = d_date_sk and
       cs_item_sk = i_item_sk and
       cs_bill_cdemo_sk = cd1.cd_demo_sk and
       cs_bill_customer_sk = c_customer_sk and
       cd1.cd_gender = 'M' and
       cd1.cd_education_status = 'Unknown' and
       c_current_cdemo_sk = cd2.cd_demo_sk and
       c_current_addr_sk = ca_address_sk and
       c_birth_month in (5,1,4,7,8,9) and
       d_year = 2002 and
       ca_state in ('AR','TX','NC'
                   ,'GA','MS','WV','AL')
 group by rollup (item.i_item_id, customer_address.ca_country, customer_address.ca_state, customer_address.ca_county)
 order by customer_address.ca_country,
        customer_address.ca_state,
        customer_address.ca_county,
	item.i_item_id, agg6
 limit 100;

-- end query 1 in stream 0 using template query18.tpl
