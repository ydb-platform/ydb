{% include 'header.sql.jinja' %}

-- NB: Subquerys
$year_total = (
 select customer.c_customer_id customer_id
       ,customer.c_first_name customer_first_name
       ,customer.c_last_name customer_last_name
       ,date_dim.d_year as year
       ,sum(ss_net_paid) year_total
       ,'s' sale_type
 from {{customer}} as customer
     cross join {{store_sales}} as store_sales
     cross join {{date_dim}} as date_dim
 where c_customer_sk = ss_customer_sk
   and ss_sold_date_sk = d_date_sk
   and d_year in (2001,2001+1)
 group by customer.c_customer_id
         ,customer.c_first_name
         ,customer.c_last_name
         ,date_dim.d_year
 union all
 select customer.c_customer_id customer_id
       ,customer.c_first_name customer_first_name
       ,customer.c_last_name customer_last_name
       ,date_dim.d_year as year
       ,sum(ws_net_paid) year_total
       ,'w' sale_type
 from {{customer}} as customer
     cross join {{web_sales}} as web_sales
     cross join {{date_dim}} as date_dim
 where c_customer_sk = ws_bill_customer_sk
   and ws_sold_date_sk = d_date_sk
   and d_year in (2001,2001+1)
 group by customer.c_customer_id
         ,customer.c_first_name
         ,customer.c_last_name
         ,date_dim.d_year
         );
-- start query 1 in stream 0 using template query74.tpl and seed 1556717815
  select
        t_s_secyear.customer_id, t_s_secyear.customer_first_name, t_s_secyear.customer_last_name
 from $year_total t_s_firstyear
      cross join $year_total t_s_secyear
     cross join $year_total t_w_firstyear
     cross join $year_total t_w_secyear
 where t_s_secyear.customer_id = t_s_firstyear.customer_id
         and t_s_firstyear.customer_id = t_w_secyear.customer_id
         and t_s_firstyear.customer_id = t_w_firstyear.customer_id
         and t_s_firstyear.sale_type = 's'
         and t_w_firstyear.sale_type = 'w'
         and t_s_secyear.sale_type = 's'
         and t_w_secyear.sale_type = 'w'
         and t_s_firstyear.year = 2001
         and t_s_secyear.year = 2001+1
         and t_w_firstyear.year = 2001
         and t_w_secyear.year = 2001+1
         and t_s_firstyear.year_total > 0
         and t_w_firstyear.year_total > 0
         and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
           > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end
 order by t_s_secyear.customer_id,t_s_secyear.customer_id,t_s_secyear.customer_id
limit 100;

-- end query 1 in stream 0 using template query74.tpl

