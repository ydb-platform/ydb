{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- NB: Used YQL's named expressions instead of ANSI SQL's "with" statement.
$customer_total_return =
-- NB: Must use correlation names (table names) in group by. And then everywhere else...
(select a.sr_customer_sk as ctr_customer_sk
,a.sr_store_sk as ctr_store_sk
-- NB: renamed "SR_FEE" -> "sr_fee"
,sum(a.sr_fee) as ctr_total_return
from {{store_returns}} as a cross join {{date_dim}} as b
where sr_returned_date_sk = d_date_sk
and d_year =2000
group by a.sr_customer_sk
,a.sr_store_sk);

$avg_total_returns = (
    select ctr_store_sk, avg(ctr_total_return)*$z1_2_35 as ctr_avg
    from $customer_total_return
    group by ctr_store_sk
);

-- -- start query 1 in stream 0 using template query1.tpl and seed 2031708268
 select  c_customer_id
from $customer_total_return ctr1
cross join {{store}}
cross join {{customer}}
-- NB: Rewrote inequality condition with explicit join
join $avg_total_returns ctr2 on ctr1.ctr_store_sk = ctr2.ctr_store_sk
where ctr_total_return > ctr_avg
and s_store_sk = ctr1.ctr_store_sk
and s_state = 'NM'
and ctr_customer_sk = c_customer_sk
order by c_customer_id
limit 100;

-- -- end query 1 in stream 0 using template query1.tpl
