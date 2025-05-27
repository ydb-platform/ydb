{% include 'header.sql.jinja' %}

-- start query 1 in stream 0 using template query4.tpl and seed 1819994127
$year_total = (select c.c_customer_id as customer_id
       ,c.c_first_name as customer_first_name
       ,c.c_last_name as customer_last_name
       ,c.c_preferred_cust_flag as customer_preferred_cust_flag
       ,c.c_birth_country as customer_birth_country
       ,c.c_login as customer_login
       ,c.c_email_address as customer_email_address
       ,d.d_year as dyear
       ,SUM(((ss.ss_ext_list_price-ss.ss_ext_wholesale_cost-ss.ss_ext_discount_amt)+ss.ss_ext_sales_price)/2) as year_total
       ,'s' as sale_type
 from {{customer}} as c
 cross join {{store_sales}} as ss
 cross join {{date_dim}} as d
 where c.c_customer_sk = ss.ss_customer_sk
   and ss.ss_sold_date_sk = d.d_date_sk
 group by c.c_customer_id
         ,c.c_first_name
         ,c.c_last_name
         ,c.c_preferred_cust_flag
         ,c.c_birth_country
         ,c.c_login
         ,c.c_email_address
         ,d.d_year
 union all
 select c.c_customer_id as customer_id
       ,c.c_first_name as customer_first_name
       ,c.c_last_name as customer_last_name
       ,c.c_preferred_cust_flag as customer_preferred_cust_flag
       ,c.c_birth_country as customer_birth_country
       ,c.c_login as customer_login
       ,c.c_email_address as customer_email_address
       ,d.d_year as dyear
       ,SUM((((cs.cs_ext_list_price-cs.cs_ext_wholesale_cost-cs.cs_ext_discount_amt)+cs.cs_ext_sales_price)/2) ) as year_total
       ,'c' as sale_type
 from {{customer}} as c
 cross join {{catalog_sales}} as cs
 cross join {{date_dim}} as d
 where c.c_customer_sk = cs.cs_bill_customer_sk
   and cs.cs_sold_date_sk = d.d_date_sk
 group by c.c_customer_id
         ,c.c_first_name
         ,c.c_last_name
         ,c.c_preferred_cust_flag
         ,c.c_birth_country
         ,c.c_login
         ,c.c_email_address
         ,d.d_year
union all
 select c.c_customer_id as customer_id
       ,c.c_first_name as customer_first_name
       ,c.c_last_name as customer_last_name
       ,c.c_preferred_cust_flag as customer_preferred_cust_flag
       ,c.c_birth_country as customer_birth_country
       ,c.c_login as customer_login
       ,c.c_email_address as customer_email_address
       ,d.d_year as dyear
       ,SUM((((ws.ws_ext_list_price-ws.ws_ext_wholesale_cost-ws.ws_ext_discount_amt)+ws.ws_ext_sales_price)/2) ) as year_total
       ,'w' as sale_type
 from {{customer}} as c
 cross join {{web_sales}} as ws
 cross join {{date_dim}} as d
 where c.c_customer_sk = ws.ws_bill_customer_sk
   and ws.ws_sold_date_sk = d.d_date_sk
 group by c.c_customer_id
         ,c.c_first_name
         ,c.c_last_name
         ,c.c_preferred_cust_flag
         ,c.c_birth_country
         ,c.c_login
         ,c.c_email_address
         ,d.d_year
         );


select
                  t_s_secyear.customer_id
                 ,t_s_secyear.customer_first_name
                 ,t_s_secyear.customer_last_name
                 ,t_s_secyear.customer_preferred_cust_flag
 from $year_total as t_s_firstyear
 cross join $year_total as t_s_secyear
 cross join $year_total as t_c_firstyear
 cross join $year_total as t_c_secyear
 cross join $year_total as t_w_firstyear
 cross join $year_total as t_w_secyear
 where t_s_secyear.customer_id = t_s_firstyear.customer_id
   and t_s_firstyear.customer_id = t_c_secyear.customer_id
   and t_s_firstyear.customer_id = t_c_firstyear.customer_id
   and t_s_firstyear.customer_id = t_w_firstyear.customer_id
   and t_s_firstyear.customer_id = t_w_secyear.customer_id
   and t_s_firstyear.sale_type = 's'
   and t_c_firstyear.sale_type = 'c'
   and t_w_firstyear.sale_type = 'w'
   and t_s_secyear.sale_type = 's'
   and t_c_secyear.sale_type = 'c'
   and t_w_secyear.sale_type = 'w'
   and t_s_firstyear.dyear =  2001
   and t_s_secyear.dyear = 2001+1
   and t_c_firstyear.dyear =  2001
   and t_c_secyear.dyear =  2001+1
   and t_w_firstyear.dyear = 2001
   and t_w_secyear.dyear = 2001+1
   and t_s_firstyear.year_total > 0
   and t_c_firstyear.year_total > 0
   and t_w_firstyear.year_total > 0
   and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end
           > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end
   and case when t_c_firstyear.year_total > 0 then t_c_secyear.year_total / t_c_firstyear.year_total else null end
           > case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
 order by t_s_secyear.customer_id
         ,t_s_secyear.customer_first_name
         ,t_s_secyear.customer_last_name
         ,t_s_secyear.customer_preferred_cust_flag
limit 100;

-- end query 1 in stream 0 using template query4.tpl
