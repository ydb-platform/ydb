{% include 'header.sql.jinja' %}

-- NB: Subquerys
$bla1 = (select ss_customer_sk customer_sk
          from {{store_sales}} as store_sales
          cross join {{date_dim}} as date_dim
          where ss_sold_date_sk = d_date_sk and
                d_year = 2001 and
                d_moy between 4 and 4+2);

$bla2 = ((select ws_bill_customer_sk customer_sk
            from {{web_sales}} as web_sales
            cross join {{date_dim}} as date_dim
            where
                  ws_sold_date_sk = d_date_sk and
                  d_year = 2001 and
                  d_moy between 4 and 4+2)
        union all
        (select cs_ship_customer_sk customer_sk
            from {{catalog_sales}} as catalog_sales
            cross join {{date_dim}} as date_dim
            where
                  cs_sold_date_sk = d_date_sk and
                  d_year = 2001 and
                  d_moy between 4 and 4+2)
        );

-- start query 1 in stream 0 using template query69.tpl and seed 797269820
select
  customer_demographics.cd_gender,
  customer_demographics.cd_marital_status,
  customer_demographics.cd_education_status,
  count(*) cnt1,
  customer_demographics.cd_purchase_estimate,
  count(*) cnt2,
  customer_demographics.cd_credit_rating,
  count(*) cnt3
 from
  {{customer}} c 
  cross join {{customer_address}} ca 
  cross join {{customer_demographics}} as customer_demographics
  left semi join $bla1 bla1 on c.c_customer_sk = bla1.customer_sk
  left only join $bla2 bla2 on c.c_customer_sk = bla2.customer_sk
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  ca_state in ('KY','GA','NM') and
  cd_demo_sk = c.c_current_cdemo_sk
 group by customer_demographics.cd_gender,
          customer_demographics.cd_marital_status,
          customer_demographics.cd_education_status,
          customer_demographics.cd_purchase_estimate,
          customer_demographics.cd_credit_rating
 order by customer_demographics.cd_gender,
          customer_demographics.cd_marital_status,
          customer_demographics.cd_education_status,
          customer_demographics.cd_purchase_estimate,
          customer_demographics.cd_credit_rating
 limit 100;

-- end query 1 in stream 0 using template query69.tpl
