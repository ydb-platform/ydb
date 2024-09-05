{% include 'header.sql.jinja' %}

-- NB: Subquerys
$blabla = (
    select substring(cast(item.i_item_desc as string),0,30) itemdesc,item.i_item_sk item_sk,date_dim.d_date solddate
  from {{store_sales}} as store_sales
      cross join {{date_dim}} as date_dim
      cross join {{item}} as item
  where ss_sold_date_sk = d_date_sk
    and ss_item_sk = i_item_sk
    and d_year in (2000,2000+1,2000+2,2000+3)
);
$frequent_ss_items =
 (select itemdesc, item_sk, solddate,count(*) cnt
  from $blabla
  group by itemdesc,item_sk,solddate
  having count(*) >4);

$max_store_sales =
 (select max(csales) tpcds_cmax
  from (select customer.c_customer_sk c_customer_sk,sum(ss_quantity*ss_sales_price) csales
        from {{store_sales}} as store_sales
            cross join {{customer}} as customer
            cross join {{date_dim}} as date_dim
        where ss_customer_sk = c_customer_sk
         and ss_sold_date_sk = d_date_sk
         and d_year in (2000,2000+1,2000+2,2000+3)
        group by customer.c_customer_sk) x);

$best_ss_customer =
 (select customer.c_customer_sk c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
  from {{store_sales}} as store_sales
      cross join {{customer}} as customer
  where ss_customer_sk = c_customer_sk
  group by customer.c_customer_sk
  having sum(ss_quantity*ss_sales_price) > $z0_95_35 * $max_store_sales);

-- start query 1 in stream 0 using template query23.tpl and seed 2031708268
select  sum(sales)
 from (select cs_quantity*cs_list_price sales
       from {{catalog_sales}} as catalog_sales
           cross join {{date_dim}} as date_dim
       where d_year = 2000
         and d_moy = 3
         and cs_sold_date_sk = d_date_sk
         and cs_item_sk in (select item_sk from $frequent_ss_items)
         and cs_bill_customer_sk in (select c_customer_sk from $best_ss_customer)
      union all
      select ws_quantity*ws_list_price sales
       from {{web_sales}} as web_sales
           cross join {{date_dim}} as date_dim
       where d_year = 2000
         and d_moy = 3
         and ws_sold_date_sk = d_date_sk
         and ws_item_sk in (select item_sk from $frequent_ss_items)
         and ws_bill_customer_sk in (select c_customer_sk from $best_ss_customer)) y
 limit 100;

select  c_last_name,c_first_name,sales
 from (select customer.c_last_name c_last_name,customer.c_first_name c_first_name,sum(cs_quantity*cs_list_price) sales
        from {{catalog_sales}} as catalog_sales
            cross join {{customer}} as customer
            cross join {{date_dim}} as date_dim
        where d_year = 2000
         and d_moy = 3
         and cs_sold_date_sk = d_date_sk
         and cs_item_sk in (select item_sk from $frequent_ss_items)
         and cs_bill_customer_sk in (select c_customer_sk from $best_ss_customer)
         and cs_bill_customer_sk = c_customer_sk
       group by customer.c_last_name,customer.c_first_name
      union all
      select customer.c_last_name c_last_name,customer.c_first_name c_first_name,sum(ws_quantity*ws_list_price) sales
       from {{web_sales}} as web_sales
           cross join {{customer}} as customer
           cross join {{date_dim}} as date_dim
       where d_year = 2000
         and d_moy = 3
         and ws_sold_date_sk = d_date_sk
         and ws_item_sk in (select item_sk from $frequent_ss_items)
         and ws_bill_customer_sk in (select c_customer_sk from $best_ss_customer)
         and ws_bill_customer_sk = c_customer_sk
       group by customer.c_last_name,customer.c_first_name) y
     order by c_last_name,c_first_name,sales
  limit 100;

-- end query 1 in stream 0 using template query23.tpl
