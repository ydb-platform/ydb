{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query45.tpl and seed 2031708268
select  customer_address.ca_zip, customer_address.ca_county, sum(ws_sales_price)
 from {{web_sales}} as web_sales
 cross join {{customer}} as customer
 cross join {{customer_address}} as customer_address
 cross join {{date_dim}} as date_dim
 cross join {{item}} as item
 where ws_bill_customer_sk = c_customer_sk
 	and c_current_addr_sk = ca_address_sk
 	and ws_item_sk = i_item_sk
 	and ( substring(cast(ca_zip as string),0,5) in ('85669', '86197','88274','83405','86475', '85392', '85460', '80348', '81792')
 	      or
 	      i_item_id in (select i_item_id
                             from {{item}} as item
                             where i_item_sk in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
                             )
 	    )
 	and ws_sold_date_sk = d_date_sk
 	and d_qoy = 1 and d_year = 1998
 group by customer_address.ca_zip, customer_address.ca_county
 order by customer_address.ca_zip, customer_address.ca_county
 limit 100;

-- end query 1 in stream 0 using template query45.tpl
