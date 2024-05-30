{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query71.tpl and seed 2031708268
select item.i_brand_id brand_id, item.i_brand brand,time_dim.t_hour,time_dim.t_minute,
 	sum(ext_price) ext_price
 from {{item}} as item cross join (select ws_ext_sales_price as ext_price,
                        ws_sold_date_sk as sold_date_sk,
                        ws_item_sk as sold_item_sk,
                        ws_sold_time_sk as time_sk
                 from {{web_sales}} as web_sales
                 cross join {{date_dim}} as date_dim
                 where d_date_sk = ws_sold_date_sk
                   and d_moy=12
                   and d_year=2000
                 union all
                 select cs_ext_sales_price as ext_price,
                        cs_sold_date_sk as sold_date_sk,
                        cs_item_sk as sold_item_sk,
                        cs_sold_time_sk as time_sk
                 from {{catalog_sales}} as catalog_sales
                 cross join {{date_dim}} as date_dim
                 where d_date_sk = cs_sold_date_sk
                   and d_moy=12
                   and d_year=2000
                 union all
                 select ss_ext_sales_price as ext_price,
                        ss_sold_date_sk as sold_date_sk,
                        ss_item_sk as sold_item_sk,
                        ss_sold_time_sk as time_sk
                 from {{store_sales}} as store_sales
                 cross join {{date_dim}} as date_dim
                 where d_date_sk = ss_sold_date_sk
                   and d_moy=12
                   and d_year=2000
                 ) tmp cross join {{time_dim}} as time_dim
 where
   sold_item_sk = item.i_item_sk
   and i_manager_id=1
   and time_sk = time_dim.t_time_sk
   and (t_meal_time = 'breakfast' or t_meal_time = 'dinner')
 group by item.i_brand, item.i_brand_id,time_dim.t_hour,time_dim.t_minute
 order by ext_price desc, brand_id, time_dim.t_hour,time_dim.t_minute
 ;

-- end query 1 in stream 0 using template query71.tpl
