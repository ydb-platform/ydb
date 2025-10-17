--!syntax_pg
--TPC-DS Q43

-- start query 1 in stream 0 using template ../query_templates/query43.tpl
select  s_store_name, s_store_id,
        sum(case when (d_day_name='Sunday') then ss_sales_price else null::numeric end) sun_sales,
        sum(case when (d_day_name='Monday') then ss_sales_price else null::numeric end) mon_sales,
        sum(case when (d_day_name='Tuesday') then ss_sales_price else null::numeric end) tue_sales,
        sum(case when (d_day_name='Wednesday') then ss_sales_price else null::numeric end) wed_sales,
        sum(case when (d_day_name='Thursday') then ss_sales_price else null::numeric end) thu_sales,
        sum(case when (d_day_name='Friday') then ss_sales_price else null::numeric end) fri_sales,
        sum(case when (d_day_name='Saturday') then ss_sales_price else null::numeric end) sat_sales
 from plato.date_dim, plato.store_sales, plato.store
 where d_date_sk = ss_sold_date_sk and
       s_store_sk = ss_store_sk and
       s_gmt_offset = -5::numeric and
       d_year = 1998 
 group by s_store_name, s_store_id
 order by s_store_name, s_store_id,sun_sales,mon_sales,tue_sales,wed_sales,thu_sales,fri_sales,sat_sales
 limit 100;

-- end query 1 in stream 0 using template ../query_templates/query43.tpl
