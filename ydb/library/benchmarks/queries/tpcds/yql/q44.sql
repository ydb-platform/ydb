{% include 'header.sql.jinja' %}

-- NB: Subquerys
$avg_net_profit = (select avg(ss_net_profit) rank_col
                                                  from {{store_sales}} as store_sales
                                                  where ss_store_sk = 366
                                                    and ss_cdemo_sk is null
                                                  group by ss_store_sk);

-- start query 1 in stream 0 using template query44.tpl and seed 1819994127
select  asceding.rnk, i1.i_product_name best_performing, i2.i_product_name worst_performing
from(select *
     from (select item_sk,rank() over (order by rank_col asc) rnk
           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col
                 from {{store_sales}} ss1
                 where ss_store_sk = 366
                 group by ss_item_sk
                 having avg(ss_net_profit) > $z0_9*$avg_net_profit)V1)V11
     where rnk  < 11) asceding
     cross join
    (select *
     from (select item_sk,rank() over (order by rank_col desc) rnk
           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col
                 from {{store_sales}} ss1
                 where ss_store_sk = 366
                 group by ss_item_sk
                 having avg(ss_net_profit) > $z0_9*$avg_net_profit)V2)V21
     where rnk  < 11) descending
     cross join
{{item}} i1 cross join
{{item}} i2
where asceding.rnk = descending.rnk
  and i1.i_item_sk=asceding.item_sk
  and i2.i_item_sk=descending.item_sk
order by asceding.rnk
limit 100;

-- end query 1 in stream 0 using template query44.tpl
