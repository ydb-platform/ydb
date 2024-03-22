{% include 'header.sql.jinja' %}

-- NB: Subquerys
$ss =
 (select customer_address.ca_county ca_county, date_dim.d_qoy d_qoy, date_dim.d_year d_year,sum(ss_ext_sales_price) as store_sales
 from {{store_sales}} as store_sales
 cross join {{date_dim}} as date_dim
 cross join {{customer_address}} as customer_address
 where ss_sold_date_sk = d_date_sk
  and ss_addr_sk=ca_address_sk
 group by customer_address.ca_county,date_dim.d_qoy, date_dim.d_year);

$ws =
 (select customer_address.ca_county ca_county,date_dim.d_qoy d_qoy, date_dim.d_year d_year,sum(ws_ext_sales_price) as web_sales
 from {{web_sales}} as web_sales 
 cross join {{date_dim}} as date_dim 
 cross join {{customer_address}} as customer_address
 where ws_sold_date_sk = d_date_sk
  and ws_bill_addr_sk=ca_address_sk
 group by customer_address.ca_county,date_dim.d_qoy, date_dim.d_year);

-- start query 1 in stream 0 using template query31.tpl and seed 1819994127
 select
        ss1.ca_county
       ,ss1.d_year
       ,ws2.web_sales/ws1.web_sales web_q1_q2_increase
       ,ss2.store_sales/ss1.store_sales store_q1_q2_increase
       ,ws3.web_sales/ws2.web_sales web_q2_q3_increase
       ,ss3.store_sales/ss2.store_sales store_q2_q3_increase
 from
        $ss ss1
        cross join $ss ss2
        cross join $ss ss3
        cross join $ws ws1
        cross join $ws ws2
        cross join $ws ws3
 where
    ss1.d_qoy = 1
    and ss1.d_year = 1999
    and ss1.ca_county = ss2.ca_county
    and ss2.d_qoy = 2
    and ss2.d_year = 1999
 and ss2.ca_county = ss3.ca_county
    and ss3.d_qoy = 3
    and ss3.d_year = 1999
    and ss1.ca_county = ws1.ca_county
    and ws1.d_qoy = 1
    and ws1.d_year = 1999
    and ws1.ca_county = ws2.ca_county
    and ws2.d_qoy = 2
    and ws2.d_year = 1999
    and ws1.ca_county = ws3.ca_county
    and ws3.d_qoy = 3
    and ws3.d_year =1999
    and case when ws1.web_sales > 0 then ws2.web_sales/ws1.web_sales else null end
       > case when ss1.store_sales > 0 then ss2.store_sales/ss1.store_sales else null end
    and case when ws2.web_sales > 0 then ws3.web_sales/ws2.web_sales else null end
       > case when ss2.store_sales > 0 then ss3.store_sales/ss2.store_sales else null end
 order by ss1.ca_county;

-- end query 1 in stream 0 using template query31.tpl
