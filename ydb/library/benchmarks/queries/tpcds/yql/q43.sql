{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query43.tpl and seed 1819994127
select  store.s_store_name, store.s_store_id,
        sum(case when (d_day_name='Sunday') then ss_sales_price else $z0 end) sun_sales,
        sum(case when (d_day_name='Monday') then ss_sales_price else $z0 end) mon_sales,
        sum(case when (d_day_name='Tuesday') then ss_sales_price else $z0 end) tue_sales,
        sum(case when (d_day_name='Wednesday') then ss_sales_price else $z0 end) wed_sales,
        sum(case when (d_day_name='Thursday') then ss_sales_price else $z0 end) thu_sales,
        sum(case when (d_day_name='Friday') then ss_sales_price else $z0 end) fri_sales,
        sum(case when (d_day_name='Saturday') then ss_sales_price else $z0 end) sat_sales
 from {{date_dim}} as date_dim 
 cross join {{store_sales}} as store_sales
 cross join {{store}} as store
 where d_date_sk = ss_sold_date_sk and
       s_store_sk = ss_store_sk and
       s_gmt_offset = -6 and
       d_year = 2001
 group by store.s_store_name, store.s_store_id
 order by store.s_store_name, store.s_store_id,sun_sales,mon_sales,tue_sales,wed_sales,thu_sales,fri_sales,sat_sales
 limit 100;

-- end query 1 in stream 0 using template query43.tpl
