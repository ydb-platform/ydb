{% include 'header.sql.jinja' %}

-- NB: Subquerys
$customer_total_return =
 (select web_returns.wr_returning_customer_sk as ctr_customer_sk
        ,customer_address.ca_state as ctr_state,
 	sum(wr_return_amt) as ctr_total_return
 from {{web_returns}} as web_returns
     cross join {{date_dim}} as date_dim
     cross join {{customer_address}} as customer_address
 where wr_returned_date_sk = d_date_sk
   and d_year =2000
   and wr_returning_addr_sk = ca_address_sk
 group by web_returns.wr_returning_customer_sk
         ,customer_address.ca_state);

$avg_total_return_by_state =
 (select ctr_state, avg(ctr_total_return) avg_return
 from $customer_total_return
 group by ctr_state);

-- start query 1 in stream 0 using template query30.tpl and seed 1819994127
  select  c_customer_id,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag
       ,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address
       ,c_last_review_date,ctr_total_return
 from $customer_total_return ctr1
     join $avg_total_return_by_state ctr2 on ctr1.ctr_state = ctr2.ctr_state
     cross join {{customer_address}} as customer_address
     cross join {{customer}} as customer
 where ctr1.ctr_total_return > $z1_2_35*ctr2.avg_return
       and ca_address_sk = c_current_addr_sk
       and ca_state = 'GA'
       and ctr1.ctr_customer_sk = c_customer_sk
 order by c_customer_id,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag
                  ,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address
                  ,c_last_review_date,ctr_total_return
limit 100;

-- end query 1 in stream 0 using template query30.tpl
