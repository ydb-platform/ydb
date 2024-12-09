{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query84.tpl and seed 1819994127
select  c_customer_id as customer_id
       , coalesce(c_last_name,'') || ', ' || coalesce(c_first_name,'') as customername
 from {{customer}} as customer
    cross join {{customer_address}} as customer_address
     cross join {{customer_demographics}} as customer_demographics
     cross join {{household_demographics}} as household_demographics
     cross join {{income_band}} as income_band
     cross join {{store_returns}} as store_returns
 where ca_city	        =  'Edgewood'
   and c_current_addr_sk = ca_address_sk
   and ib_lower_bound   >=  38128
   and ib_upper_bound   <=  38128 + 50000
   and ib_income_band_sk = hd_income_band_sk
   and cd_demo_sk = c_current_cdemo_sk
   and hd_demo_sk = c_current_hdemo_sk
   and sr_cdemo_sk = cd_demo_sk
 order by customer_id
 limit 100;

-- end query 1 in stream 0 using template query84.tpl
