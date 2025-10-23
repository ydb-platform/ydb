--!syntax_pg
--TPC-DS Q13

-- start query 1 in stream 0 using template ../query_templates/query13.tpl
select avg(ss_quantity) avg_ss_q
       ,avg(ss_ext_sales_price) avg_ss_s
       ,avg(ss_ext_wholesale_cost) avg_ss_w
       ,sum(ss_ext_wholesale_cost) sum_ss_w
 from plato.store_sales
     ,plato.store
     ,plato.customer_demographics
     ,plato.household_demographics
     ,plato.customer_address
     ,plato.date_dim
 where s_store_sk = ss_store_sk
 and  ss_sold_date_sk = d_date_sk and d_year = 2001
 and((ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'D'
  and cd_education_status = '2 yr Degree'
  and ss_sales_price between 100.00::numeric and 150.00::numeric
  and hd_dep_count = 3   
     )or
     (ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'S'
  and cd_education_status = 'Secondary'
  and ss_sales_price between 50.00::numeric and 100.00::numeric   
  and hd_dep_count = 1
     ) or 
     (ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'W'
  and cd_education_status = 'Advanced Degree'
  and ss_sales_price between 150.00::numeric and 200.00::numeric 
  and hd_dep_count = 1  
     ))
 and((ss_addr_sk = ca_address_sk
  and ca_country = 'United States'
  and ca_state in ('CO', 'IL', 'MN')
  and ss_net_profit between 100::numeric and 200::numeric  
     ) or
     (ss_addr_sk = ca_address_sk
  and ca_country = 'United States'
  and ca_state in ('OH', 'MT', 'NM')
  and ss_net_profit between 150::numeric and 300::numeric  
     ) or
     (ss_addr_sk = ca_address_sk
  and ca_country = 'United States'
  and ca_state in ('TX', 'MO', 'MI')
  and ss_net_profit between 50::numeric and 250::numeric
     ))
;

-- end query 1 in stream 0 using template ../query_templates/query13.tpl
