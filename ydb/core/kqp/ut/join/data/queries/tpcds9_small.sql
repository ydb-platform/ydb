pragma TablePathPrefix = "/Root/test/ds/";

---$count_1_20 = (select count(*)
--    from store_sales as store_sales
--    where ss_quantity between 1 and 20);

$avg_ss_ext_discount_amt_1_20 = (select avg(ss_ext_discount_amt)
    from store_sales as store_sales
    where ss_quantity between 1 and 20);

$avg_ss_net_profit_1_20 = (select avg(ss_net_profit)
   from store_sales as store_sales
    where ss_quantity between 1 and 20);


--select case when $count_1_20 > 98972190
select case when 1e20 > 98972190
            then $avg_ss_ext_discount_amt_1_20
            else $avg_ss_net_profit_1_20 end bucket1

from reason as reason
where r_reason_sk = 1;