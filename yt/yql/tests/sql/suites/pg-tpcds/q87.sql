--!syntax_pg
--TPC-DS Q87

-- start query 1 in stream 0 using template ../query_templates/query87.tpl
select count(*) 
from ((select distinct c_last_name, c_first_name, d_date
       from plato.store_sales, plato.date_dim, plato.customer
       where store_sales.ss_sold_date_sk = date_dim.d_date_sk
         and store_sales.ss_customer_sk = customer.c_customer_sk
         and d_month_seq between 1212 and 1212+11)
       except
      (select distinct c_last_name, c_first_name, d_date
       from plato.catalog_sales, plato.date_dim, plato.customer
       where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
         and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1212 and 1212+11)
       except
      (select distinct c_last_name, c_first_name, d_date
       from plato.web_sales, plato.date_dim, plato.customer
       where web_sales.ws_sold_date_sk = date_dim.d_date_sk
         and web_sales.ws_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1212 and 1212+11)
) cool_cust
;

-- end query 1 in stream 0 using template ../query_templates/query87.tpl
