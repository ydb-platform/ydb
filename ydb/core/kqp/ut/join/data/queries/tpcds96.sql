pragma TablePathPrefix = "/Root/test/ds/";
-- NB: Subquerys
-- start query 1 in stream 0 using template query96.tpl and seed 1819994127
select  count(*) bla
from store_sales
    cross join household_demographics
    cross join time_dim cross join store
where ss_sold_time_sk = time_dim.t_time_sk
    and ss_hdemo_sk = household_demographics.hd_demo_sk
    and ss_store_sk = s_store_sk
    and time_dim.t_hour = 16
    and time_dim.t_minute >= 30
    and household_demographics.hd_dep_count = 6
    and store.s_store_name = 'ese'
order by bla
limit 100;
