{% include 'header.sql.jinja' %}

-- NB: Subquerys

-- start query 1 in stream 0 using template query13.tpl and seed 622697896
select avg(ss_quantity)
       ,avg(ss_ext_sales_price)
       ,avg(ss_ext_wholesale_cost)
       ,sum(ss_ext_wholesale_cost)
 from {{store_sales}} as store_sales
     cross join {{store}} as store
     cross join {{customer_demographics}} as customer_demographics
     cross join {{household_demographics}} as household_demographics
     cross join {{customer_address}} as customer_address
     cross join {{date_dim}} as date_dim
 where s_store_sk = ss_store_sk
 and  ss_sold_date_sk = d_date_sk and d_year = 2001
 and((ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'U'
  and cd_education_status = 'Secondary'
  and ss_sales_price between 100 and 150
  and hd_dep_count = 3
     )or
     (ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'W'
  and cd_education_status = 'College'
  and ss_sales_price between 50 and 100
  and hd_dep_count = 1
     ) or
     (ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'D'
  and cd_education_status = 'Primary'
  and ss_sales_price between 150 and 200
  and hd_dep_count = 1
     ))
 and((ss_addr_sk = ca_address_sk
  and ca_country = 'United States'
  and ca_state in ('TX', 'OK', 'MI')
  and ss_net_profit between 100 and 200
     ) or
     (ss_addr_sk = ca_address_sk
  and ca_country = 'United States'
  and ca_state in ('WA', 'NC', 'OH')
  and ss_net_profit between 150 and 300
     ) or
     (ss_addr_sk = ca_address_sk
  and ca_country = 'United States'
  and ca_state in ('MT', 'FL', 'GA')
  and ss_net_profit between 50 and 250
     ))
;

-- end query 1 in stream 0 using template query13.tpl
