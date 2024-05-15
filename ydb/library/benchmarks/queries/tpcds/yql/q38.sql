{% include 'header.sql.jinja' %}

-- NB: Subquerys
$bla1 = (select distinct c_last_name, c_first_name, d_date
    from {{store_sales}} as store_sales
    cross join {{date_dim}} as date_dim
    cross join {{customer}} as customer
          where store_sales.ss_sold_date_sk = date_dim.d_date_sk
      and store_sales.ss_customer_sk = customer.c_customer_sk
      and d_month_seq between 1186 and 1186 + 11);

$bla2 = (select distinct c_last_name, c_first_name, d_date
    from {{catalog_sales}} as catalog_sales 
    cross join {{date_dim}} as date_dim
    cross join {{customer}} as customer
          where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
      and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
      and d_month_seq between 1186 and 1186 + 11);

$bla3 = (select distinct c_last_name, c_first_name, d_date
    from {{web_sales}} as web_sales
    cross join {{date_dim}} as date_dim
    cross join {{customer}} as customer
          where web_sales.ws_sold_date_sk = date_dim.d_date_sk
      and web_sales.ws_bill_customer_sk = customer.c_customer_sk
      and d_month_seq between 1186 and 1186 + 11);

-- start query 1 in stream 0 using template query38.tpl and seed 1819994127

select count(*) from
((select * from ( select bla1.c_last_name as c_last_name, bla1.c_first_name as c_first_name, bla1.d_date as d_date from
    $bla1 bla1 join $bla2 bla2
    on bla1.c_first_name = bla2.c_first_name and
       bla1.d_date = bla2.d_date
    join any $bla3 bla3
    on bla1.c_first_name = bla3.c_first_name and
       bla1.d_date = bla3.d_date
    where bla1.c_last_name is null and bla2.c_last_name is null and bla3.c_last_name is null
) hot_cust)
union
(select * from ( select bla1.c_last_name as c_last_name, bla1.c_first_name as c_first_name, bla1.d_date as d_date from
    $bla1 bla1 join $bla2 bla2
    on bla1.d_date = bla2.d_date
    join any $bla3 bla3
    on bla1.d_date = bla3.d_date
    where bla1.c_last_name is null and bla2.c_last_name is null and bla3.c_last_name is null
    and  bla1.c_first_name is null and bla2.c_first_name is null and bla3.c_first_name is null
) hot_cust)
union
(select * from ( select bla1.c_last_name as c_last_name, bla1.c_first_name as c_first_name, bla1.d_date as d_date from
    $bla1 bla1 join any $bla2 bla2
    on bla1.c_last_name = bla2.c_last_name and
       bla1.c_first_name = bla2.c_first_name and
       bla1.d_date = bla2.d_date
    join any $bla3 bla3
    on bla1.c_last_name = bla3.c_last_name and
       bla1.c_first_name = bla3.c_first_name and
       bla1.d_date = bla3.d_date
) hot_cust))
limit 100;

-- end query 1 in stream 0 using template query38.tpl
