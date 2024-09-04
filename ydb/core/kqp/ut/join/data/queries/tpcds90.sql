pragma TablePathPrefix = "/Root/test/ds/";
-- NB: Subquerys
-- start query 1 in stream 0 using template query90.tpl and seed 2031708268
select  cast(amc as decimal(15,4))/cast(pmc as decimal(15,4)) am_pm_ratio
 from ( select count(*) amc
       from web_sales cross join household_demographics cross join time_dim cross join web_page
       where ws_sold_time_sk = time_dim.t_time_sk
         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
         and ws_web_page_sk = web_page.wp_web_page_sk
         and time_dim.t_hour between 9 and 9+1
         and household_demographics.hd_dep_count = 3
         and web_page.wp_char_count between 5000 and 5200) at cross join
      ( select count(*) pmc
       from web_sales cross join household_demographics cross join time_dim cross join web_page
       where ws_sold_time_sk = time_dim.t_time_sk
         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
         and ws_web_page_sk = web_page.wp_web_page_sk
         and time_dim.t_hour between 16 and 16+1
         and household_demographics.hd_dep_count = 3
         and web_page.wp_char_count between 5000 and 5200) pt
 order by am_pm_ratio
 limit 100;
 