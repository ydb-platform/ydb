{% include 'header.sql.jinja' %}

-- NB: Subquerys
$bla1 = (select ss_customer_sk customer_sk
          from {{store_sales}} as store_sales
          cross join {{date_dim}} as date_dim
          where ss_sold_date_sk = d_date_sk and
                d_year = 2001 and
                d_qoy < 4);
$bla2 = (
    (select ws_bill_customer_sk customer_sk
            from {{web_sales}} as web_sales
            cross join {{date_dim}} as date_dim
            where ws_sold_date_sk = d_date_sk and
                  d_year = 2001 and
                  d_qoy < 4)
    union all
    (select cs_ship_customer_sk customer_sk
            from {{catalog_sales}} as catalog_sales
            cross join {{date_dim}} as date_dim
            where cs_sold_date_sk = d_date_sk and
                  d_year = 2001 and
                  d_qoy < 4)
);

-- start query 1 in stream 0 using template query35.tpl and seed 1930872976
select
  ca.ca_state,
  customer_demographics.cd_gender,
  customer_demographics.cd_marital_status,
  customer_demographics.cd_dep_count,
  count(*) cnt1,
  avg(customer_demographics.cd_dep_count),
  min(customer_demographics.cd_dep_count),
  sum(customer_demographics.cd_dep_count),
  customer_demographics.cd_dep_employed_count,
  count(*) cnt2,
  avg(customer_demographics.cd_dep_employed_count),
  min(customer_demographics.cd_dep_employed_count),
  sum(cd_dep_employed_count),
  customer_demographics.cd_dep_college_count,
  count(*) cnt3,
  avg(customer_demographics.cd_dep_college_count),
  min(customer_demographics.cd_dep_college_count),
  sum(customer_demographics.cd_dep_college_count)
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
