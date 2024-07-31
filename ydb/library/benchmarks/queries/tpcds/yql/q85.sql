{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query85.tpl and seed 622697896
select  substring(cast(reason.r_reason_desc as string),0,20) bla
       ,avg(ws_quantity) bla2
       ,avg(wr_refunded_cash) bla3
       ,avg(wr_fee) bla4
 from {{web_sales}} as web_sales
 cross join {{web_returns}} as web_returns 
 cross join {{web_page}} as web_page
 cross join {{customer_demographics}} cd1 cross join
      {{customer_demographics}} cd2 
      cross join {{customer_address}} as customer_address
      cross join {{date_dim}} as date_dim
      cross join {{reason}} as reason
 where ws_web_page_sk = wp_web_page_sk
   and ws_item_sk = wr_item_sk
   and ws_order_number = wr_order_number
   and ws_sold_date_sk = d_date_sk and d_year = 2001
   and cd1.cd_demo_sk = wr_refunded_cdemo_sk
   and cd2.cd_demo_sk = wr_returning_cdemo_sk
   and ca_address_sk = wr_refunded_addr_sk
   and r_reason_sk = wr_reason_sk
   and
   (
    (
     cd1.cd_marital_status = 'M'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = '4 yr Degree'
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 100 and 150
    )
   or
    (
     cd1.cd_marital_status = 'S'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = 'College'
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 50 and 100
    )
   or
    (
     cd1.cd_marital_status = 'D'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = 'Secondary'
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 150 and 200
    )
   )
   and
   (
    (
     ca_country = 'United States'
     and
     ca_state in ('TX', 'VA', 'CA')
     and ws_net_profit between 100 and 200
    )
    or
    (
     ca_country = 'United States'
     and
     ca_state in ('AR', 'NE', 'MO')
     and ws_net_profit between 150 and 300
    )
    or
    (
     ca_country = 'United States'
     and
     ca_state in ('IA', 'MS', 'WA')
     and ws_net_profit between 50 and 250
    )
   )
group by reason.r_reason_desc
order by bla
        ,bla2
        ,bla3
        ,bla4
limit 100;

-- end query 1 in stream 0 using template query85.tpl
