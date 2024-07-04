{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query79.tpl and seed 2031708268
select
  c_last_name,c_first_name,substring(cast(s_city as string),0,30) bla,ss_ticket_number,amt,profit
  from
   (select store_sales.ss_ticket_number ss_ticket_number
          ,store_sales.ss_customer_sk ss_customer_sk
          ,store.s_city s_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
    from {{store_sales}} as store_sales
    cross join {{date_dim}} as date_dim
    cross join {{store}} as store
    cross join {{household_demographics}} as household_demographics
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and (household_demographics.hd_dep_count = 0 or household_demographics.hd_vehicle_count > 3)
    and date_dim.d_dow = 1
    and date_dim.d_year in (1998,1998+1,1998+2)
    and store.s_number_employees between 200 and 295
    group by store_sales.ss_ticket_number,store_sales.ss_customer_sk,store_sales.ss_addr_sk,store.s_city) ms cross join {{customer}} as customer
    where ss_customer_sk = c_customer_sk
 order by c_last_name,c_first_name,bla, profit
        , ss_ticket_number, amt
limit 100;

-- end query 1 in stream 0 using template query79.tpl
