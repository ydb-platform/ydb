{% include 'header.sql.jinja' %}

-- NB: Subquerys
$bla1 = (select ss_customer_sk customer_sk
          from {{store_sales}} as store_sales
          cross join {{date_dim}} as date_dim
          where ss_sold_date_sk = d_date_sk and
                d_year = 2002 and
                d_qoy < 4);
$bla2 = (
    (select ws_bill_customer_sk customer_sk
            from {{web_sales}} as web_sales
            cross join {{date_dim}} as date_dim
            where ws_sold_date_sk = d_date_sk and
                  d_year = 2002 and
                  d_qoy < 4)
    union all
    (select cs_ship_customer_sk customer_sk
            from {{catalog_sales}} as catalog_sales
            cross join {{date_dim}} as date_dim
            where cs_sold_date_sk = d_date_sk and
                  d_year = 2002 and
                  d_qoy < 4)
);

-- start query 1 in stream 0 using template query35.tpl and seed 1930872976
select
  ca.ca_state,
  customer_demographics.cd_gender,
  customer_demographics.cd_marital_status,
  customer_demographics.cd_dep_count,
  count(*) cnt1,
  min(customer_demographics.cd_dep_count) a1,
  max(customer_demographics.cd_dep_count) x1,
  avg(customer_demographics.cd_dep_count) s1,
  customer_demographics.cd_dep_employed_count,
  count(*) cnt2,
  min(customer_demographics.cd_dep_employed_count) a2,
  max(customer_demographics.cd_dep_employed_count) x2,
  avg(customer_demographics.cd_dep_employed_count) s2,
  customer_demographics.cd_dep_college_count,
  count(*) cnt3,
  min(customer_demographics.cd_dep_college_count) a3,
  max(customer_demographics.cd_dep_college_count) x3,
  avg(customer_demographics.cd_dep_college_count) s3
 from
  {{customer}} c 
  cross join {{customer_address}} ca 
  cross join {{customer_demographics}} as customer_demographics
  left semi join $bla1 bla1 on c.c_customer_sk = bla1.customer_sk
  left semi join $bla2 bla2 on c.c_customer_sk = bla2.customer_sk
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  cd_demo_sk = c.c_current_cdemo_sk
group by ca.ca_state,
          customer_demographics.cd_gender,
          customer_demographics.cd_marital_status,
          customer_demographics.cd_dep_count,
          customer_demographics.cd_dep_employed_count,
          customer_demographics.cd_dep_college_count
 order by ca.ca_state,
          customer_demographics.cd_gender,
          customer_demographics.cd_marital_status,
          customer_demographics.cd_dep_count,
          customer_demographics.cd_dep_employed_count,
          customer_demographics.cd_dep_college_count
 limit 100;

-- end query 1 in stream 0 using template query35.tpl
