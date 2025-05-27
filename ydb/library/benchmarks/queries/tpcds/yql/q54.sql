{% include 'header.sql.jinja' %}

-- start query 1 in stream 0 using template query54.tpl and seed 1930872976
$d_month_seq_begin = (select distinct d_month_seq+1
                                 from   {{date_dim}} as date_dim where d_year = 1998 and d_moy = 12);
$d_month_seq_end = (select distinct d_month_seq+3
                                 from   {{date_dim}} as date_dim where d_year = 1998 and d_moy = 12);

$my_customers = (
 select distinct c_customer_sk
        , c_current_addr_sk
 from
        ( select cs_sold_date_sk sold_date_sk,
                 cs_bill_customer_sk customer_sk,
                 cs_item_sk item_sk
          from   {{catalog_sales}} as catalog_sales
          union all
          select ws_sold_date_sk sold_date_sk,
                 ws_bill_customer_sk customer_sk,
                 ws_item_sk item_sk
          from   {{web_sales}} as web_sales
         ) cs_or_ws_sales
cross join
         {{item}} as item
cross join
         {{date_dim}} as date_dim
cross join
         {{customer}} as customer
 where   sold_date_sk = d_date_sk
         and item_sk = i_item_sk
         and i_category = 'Women'
         and i_class = 'maternity'
         and c_customer_sk = cs_or_ws_sales.customer_sk
         and d_moy = 12
         and d_year = 1998
 );

$my_revenue = (
 select my_customers.c_customer_sk,
        sum(ss_ext_sales_price) as revenue
 from   $my_customers as my_customers
 cross join
        {{store_sales}} as store_sales
cross join
        {{customer_address}} as customer_address
cross join
        {{store}} as store
cross join
        {{date_dim}} as date_dim
 where  my_customers.c_current_addr_sk = customer_address.ca_address_sk
        and ca_county = s_county
        and ca_state = s_state
        and ss_sold_date_sk = d_date_sk
        and c_customer_sk = ss_customer_sk
        and d_month_seq between $d_month_seq_begin
                           and  $d_month_seq_end
 group by my_customers.c_customer_sk
 );

 $segments =
 (select cast((revenue/50) as int) as segment
  from   $my_revenue
 );

  select  segment, count(*) as num_customers, segment*50 as segment_base
 from $segments
 group by segment
 order by segment, num_customers
 limit 100;

-- end query 1 in stream 0 using template query54.tpl
