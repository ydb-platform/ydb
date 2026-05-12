--!syntax_pg
--TPC-DS Q23

-- start query 1 in stream 0 using template ../query_templates/query23.tpl
with frequent_ss_items as 
 (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
  from plato.store_sales
      ,plato.date_dim 
      ,plato.item
  where ss_sold_date_sk = d_date_sk
    and ss_item_sk = i_item_sk 
    and d_year in (1999,1999+1,1999+2,1999+3)
  group by substr(i_item_desc,1,30),i_item_sk,d_date
  having count(*) >4),
 max_store_sales as
 (select max(csales) tpcds_cmax 
  from (select c_customer_sk,sum(ss_quantity::numeric*ss_sales_price) csales
        from plato.store_sales
            ,plato.customer
            ,plato.date_dim 
        where ss_customer_sk = c_customer_sk
         and ss_sold_date_sk = d_date_sk
         and d_year in (1999,1999+1,1999+2,1999+3) 
        group by c_customer_sk) a),
 best_ss_customer as
 (select c_customer_sk,sum(ss_quantity::numeric*ss_sales_price) ssales
  from plato.store_sales
      ,plato.customer
  where ss_customer_sk = c_customer_sk
  group by c_customer_sk
  having sum(ss_quantity::numeric*ss_sales_price) > (95.0/100.0)::numeric * (select
  *
from
 max_store_sales)::numeric)
  select  sum(sales)
 from (select cs_quantity::numeric*cs_list_price sales
       from plato.catalog_sales
           ,plato.date_dim 
       where d_year = 1999 
         and d_moy = 1 
         and cs_sold_date_sk = d_date_sk 
         and cs_item_sk in (select item_sk from frequent_ss_items)
         and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer)
      union all
      select ws_quantity::numeric*ws_list_price sales
       from plato.web_sales 
           ,plato.date_dim 
       where d_year = 1999 
         and d_moy = 1 
         and ws_sold_date_sk = d_date_sk 
         and ws_item_sk in (select item_sk from frequent_ss_items)
         and ws_bill_customer_sk in (select c_customer_sk from best_ss_customer)) a 
 limit 100;
with frequent_ss_items as
 (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
  from plato.store_sales
      ,plato.date_dim
      ,plato.item
  where ss_sold_date_sk = d_date_sk
    and ss_item_sk = i_item_sk
    and d_year in (1999,1999 + 1,1999 + 2,1999 + 3)
  group by substr(i_item_desc,1,30),i_item_sk,d_date
  having count(*) >4),
 max_store_sales as
 (select max(csales) tpcds_cmax
  from (select c_customer_sk,sum(ss_quantity::numeric*ss_sales_price) csales
        from plato.store_sales
            ,plato.customer
            ,plato.date_dim 
        where ss_customer_sk = c_customer_sk
         and ss_sold_date_sk = d_date_sk
         and d_year in (1999,1999+1,1999+2,1999+3)
        group by c_customer_sk) a),
 best_ss_customer as
 (select c_customer_sk,sum(ss_quantity::numeric*ss_sales_price) ssales
  from plato.store_sales
      ,plato.customer
  where ss_customer_sk = c_customer_sk
  group by c_customer_sk
  having sum(ss_quantity::numeric*ss_sales_price) > (95.0/100.0)::numeric * (select
  *
 from max_store_sales)::numeric)
  select  c_last_name,c_first_name,sales
 from (select c_last_name,c_first_name,sum(cs_quantity::numeric*cs_list_price) sales
        from plato.catalog_sales
            ,plato.customer
            ,plato.date_dim 
        where d_year = 1999 
         and d_moy = 1 
         and cs_sold_date_sk = d_date_sk 
         and cs_item_sk in (select item_sk from frequent_ss_items)
         and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer)
         and cs_bill_customer_sk = c_customer_sk 
       group by c_last_name,c_first_name
      union all
      select c_last_name,c_first_name,sum(ws_quantity::numeric*ws_list_price) sales
       from plato.web_sales
           ,plato.customer
           ,plato.date_dim 
       where d_year = 1999 
         and d_moy = 1 
         and ws_sold_date_sk = d_date_sk 
         and ws_item_sk in (select item_sk from frequent_ss_items)
         and ws_bill_customer_sk in (select c_customer_sk from best_ss_customer)
         and ws_bill_customer_sk = c_customer_sk
       group by c_last_name,c_first_name) a
     order by c_last_name,c_first_name,sales
  limit 100;

-- end query 1 in stream 0 using template ../query_templates/query23.tpl
