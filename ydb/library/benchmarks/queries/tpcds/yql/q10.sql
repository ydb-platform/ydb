{% include 'header.sql.jinja' %}

-- NB: Subquerys
$bla1 = (select DISTINCT ss_customer_sk
          from {{store_sales}} as store_sales
          cross join {{date_dim}} as date_dim
          where ss_sold_date_sk = d_date_sk and
                d_year = 2000 and
                d_moy between 3 and 3+3);
$bla2 = (select DISTINCT customer_sk from ((select ws_bill_customer_sk customer_sk
            from {{web_sales}} as web_sales
            cross join {{date_dim}} as date_dim
            where ws_sold_date_sk = d_date_sk and
                  d_year = 2000 and
                  d_moy between 3 ANd 3+3)
        union all
        (select cs_ship_customer_sk customer_sk
            from {{catalog_sales}} as catalog_sales
            cross join {{date_dim}} as date_dim
            where
                  cs_sold_date_sk = d_date_sk and
                  d_year = 2000 and
                  d_moy between 3 and 3+3)));

-- start query 1 in stream 0 using template query10.tpl and seed 797269820
select
  customer_demographics.cd_gender,
  customer_demographics.cd_marital_status,
  customer_demographics.cd_education_status,
  count(*) cnt1,
  customer_demographics.cd_purchase_estimate,
  count(*) cnt2,
  customer_demographics.cd_credit_rating,
  count(*) cnt3,
  customer_demographics.cd_dep_count,
  count(*) cnt4,
  customer_demographics.cd_dep_employed_count,
  count(*) cnt5,
  customer_demographics.cd_dep_college_count,
  count(*) cnt6
 from
  {{customer}} c 
  cross join {{customer_address}} ca 
  cross join {{customer_demographics}} as customer_demographics
  left semi join $bla1 bla1 on (c.c_customer_sk = bla1.ss_customer_sk)
  left semi join $bla2 bla2 on (c.c_customer_sk = bla2.customer_sk)
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  ca_county in ('Fillmore County','McPherson County','Bonneville County','Boone County','Brown County') and
  cd_demo_sk = c.c_current_cdemo_sk
 group by customer_demographics.cd_gender,
          customer_demographics.cd_marital_status,
          customer_demographics.cd_education_status,
          customer_demographics.cd_purchase_estimate,
          customer_demographics.cd_credit_rating,
          customer_demographics.cd_dep_count,
          customer_demographics.cd_dep_employed_count,
          customer_demographics.cd_dep_college_count
 order by customer_demographics.cd_gender,
          customer_demographics.cd_marital_status,
          customer_demographics.cd_education_status,
          customer_demographics.cd_purchase_estimate,
          customer_demographics.cd_credit_rating,
          customer_demographics.cd_dep_count,
          customer_demographics.cd_dep_employed_count,
          customer_demographics.cd_dep_college_count
limit 100;

-- end query 1 in stream 0 using template query10.tpl
