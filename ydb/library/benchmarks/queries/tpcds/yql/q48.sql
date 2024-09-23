{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query48.tpl and seed 622697896
select sum (ss_quantity)
 from {{store_sales}} as store_sales
 cross join {{store}} as store
 cross join {{customer_demographics}} as customer_demographics
 cross join {{customer_address}} as customer_address
 cross join {{date_dim}} as date_dim
 where s_store_sk = ss_store_sk
 and  ss_sold_date_sk = d_date_sk and d_year = 1998
 and
 (
  (
   cd_demo_sk = ss_cdemo_sk
   and
   cd_marital_status = 'M'
   and
   cd_education_status = 'Unknown'
   and
   ss_sales_price between 100 and 150
   )
 or
  (
  cd_demo_sk = ss_cdemo_sk
   and
   cd_marital_status = 'W'
   and
   cd_education_status = 'College'
   and
   ss_sales_price between 50 and 100
  )
 or
 (
  cd_demo_sk = ss_cdemo_sk
  and
   cd_marital_status = 'D'
   and
   cd_education_status = 'Primary'
   and
   ss_sales_price between 150 and 200
 )
 )
 and
 (
  (
  ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('MI', 'GA', 'NH')
  and ss_net_profit between 0 and 2000
  )
 or
  (ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('TX', 'KY', 'SD')
  and ss_net_profit between 150 and 3000
  )
 or
  (ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('NY', 'OH', 'FL')
  and ss_net_profit between 50 and 25000
  )
 )
;

-- end query 1 in stream 0 using template query48.tpl
