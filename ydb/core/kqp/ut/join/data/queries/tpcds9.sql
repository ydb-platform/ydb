pragma TablePathPrefix = "/Root/test/ds/";

$count_1_20 = (select count(*)
    from store_sales as store_sales
    where ss_quantity between 1 and 20);

$avg_ss_ext_discount_amt_1_20 = (select avg(ss_ext_discount_amt)
    from store_sales as store_sales
    where ss_quantity between 1 and 20);

$avg_ss_net_profit_1_20 = (select avg(ss_net_profit)
    from store_sales as store_sales
    where ss_quantity between 1 and 20);


$count_21_40 = (select count(*)
    from store_sales as store_sales
    where ss_quantity between 21 and 40);

$avg_ss_ext_discount_amt_21_40 = (select avg(ss_ext_discount_amt)
    from store_sales as store_sales
    where ss_quantity between 21 and 40);

$avg_ss_net_profit_21_40 = (select avg(ss_net_profit)
    from store_sales as store_sales
    where ss_quantity between 21 and 40);


$count_41_60 = (select count(*)
    from store_sales as store_sales
    where ss_quantity between 41 and 60);

$avg_ss_ext_discount_amt_41_60 = (select avg(ss_ext_discount_amt)
    from store_sales as store_sales
    where ss_quantity between 41 and 60);

$avg_ss_net_profit_41_60 = (select avg(ss_net_profit)
    from store_sales as store_sales
    where ss_quantity between 41 and 60);


$count_61_80 = (select count(*)
    from store_sales as store_sales
    where ss_quantity between 61 and 80);

$avg_ss_ext_discount_amt_61_80 = (select avg(ss_ext_discount_amt)
    from store_sales as store_sales
    where ss_quantity between 61 and 80);

$avg_ss_net_profit_61_80 = (select avg(ss_net_profit)
    from store_sales as store_sales
    where ss_quantity between 61 and 80);


$count_81_100 = (select count(*)
    from store_sales as store_sales
    where ss_quantity between 81 and 100);

$avg_ss_ext_discount_amt_81_100 = (select avg(ss_ext_discount_amt)
    from store_sales as store_sales
    where ss_quantity between 81 and 100);

$avg_ss_net_profit_81_100 = (select avg(ss_net_profit)
    from store_sales as store_sales
    where ss_quantity between 81 and 100);


select case when $count_1_20 > 98972190
            then $avg_ss_ext_discount_amt_1_20
            else $avg_ss_net_profit_1_20 end bucket1 ,
       case when $count_21_40 > 160856845
            then $avg_ss_ext_discount_amt_21_40
            else $avg_ss_net_profit_21_40 end bucket2,
       case when $count_41_60 > 12733327
            then $avg_ss_ext_discount_amt_41_60
            else $avg_ss_net_profit_41_60 end bucket3,
       case when $count_61_80 > 96251173
            then $avg_ss_ext_discount_amt_61_80
            else $avg_ss_net_profit_61_80 end bucket4,
       case when $count_81_100 > 80049606
            then $avg_ss_ext_discount_amt_81_100
            else $avg_ss_net_profit_81_100 end bucket5
from reason as reason
where r_reason_sk = 1;