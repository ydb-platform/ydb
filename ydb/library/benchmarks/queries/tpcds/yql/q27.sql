{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query27.tpl and seed 2017787633
select  item.i_item_id,
        store.s_state, grouping(store.s_state) g_state,
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4
 from {{store_sales}} as store_sales
 cross join {{customer_demographics}} as customer_demographics
 cross join {{date_dim}} as date_dim
 cross join {{store}} as store
 cross join {{item}} as item
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_store_sk = s_store_sk and
       ss_cdemo_sk = cd_demo_sk and
       cd_gender = 'F' and
       cd_marital_status = 'U' and
       cd_education_status = '2 yr Degree' and
       d_year = 2000 and
       s_state in ('AL','IN', 'SC', 'NY', 'OH', 'FL')
 group by rollup (item.i_item_id, store.s_state)
 order by item.i_item_id
         ,store.s_state
 limit 100;

-- end query 1 in stream 0 using template query27.tpl
