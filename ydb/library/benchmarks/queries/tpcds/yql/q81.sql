{% include 'header.sql.jinja' %}

-- NB: Subquerys
$customer_total_return =
 (select catalog_returns.cr_returning_customer_sk as ctr_customer_sk
        ,customer_address.ca_state as ctr_state,
 	sum(cr_return_amt_inc_tax) as ctr_total_return
 from {{catalog_returns}} as catalog_returns
      cross join {{date_dim}} as date_dim
     cross join {{customer_address}} as customer_address
 where cr_returned_date_sk = d_date_sk
   and d_year =1998
   and cr_returning_addr_sk = ca_address_sk
 group by catalog_returns.cr_returning_customer_sk
         ,customer_address.ca_state );
$avg_ctr_total_return = (select ctr_state, avg(ctr_total_return) as ctr_total_return from $customer_total_return group by ctr_state);
-- start query 1 in stream 0 using template query81.tpl and seed 1819994127
  select  c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name
                   ,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset
                  ,ca_location_type,ctr1.ctr_total_return
 from $customer_total_return ctr1
     join $avg_ctr_total_return ctr2 on (ctr1.ctr_state = ctr2.ctr_state)
     cross join {{customer_address}} as customer_address
cross join {{customer}} as customer
 where ctr1.ctr_total_return > ctr2.ctr_total_return*$z1_2_35
       and ca_address_sk = c_current_addr_sk
       and ca_state = 'TX'
       and ctr1.ctr_customer_sk = c_customer_sk
 order by c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name
                   ,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset
                  ,ca_location_type,ctr1.ctr_total_return
 limit 100;

-- end query 1 in stream 0 using template query81.tpl
