{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query46.tpl and seed 803547492
select  c_last_name
       ,c_first_name
       ,ca_city
       ,bought_city
       ,ss_ticket_number
       ,amt,profit
 from
   (select store_sales.ss_ticket_number ss_ticket_number
          ,store_sales.ss_customer_sk ss_customer_sk
          ,customer_address.ca_city bought_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
    from {{store_sales}} as store_sales
    cross join {{date_dim}} as date_dim
    cross join {{store}} as store
    cross join {{household_demographics}} as household_demographics
    cross join {{customer_address}} as customer_address
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and store_sales.ss_addr_sk = customer_address.ca_address_sk
    and (household_demographics.hd_dep_count = 0 or
         household_demographics.hd_vehicle_count= 1)
    and date_dim.d_dow in (6,0)
    and date_dim.d_year in (2000,2000+1,2000+2)
    and store.s_city in ('Five Forks','Oakland','Fairview','Winchester','Farmington')
    group by store_sales.ss_ticket_number,store_sales.ss_customer_sk,store_sales.ss_addr_sk,customer_address.ca_city) dn 
    cross join {{customer}} as customer
    cross join {{customer_address}} current_addr
    where ss_customer_sk = c_customer_sk
      and customer.c_current_addr_sk = current_addr.ca_address_sk
      and current_addr.ca_city <> bought_city
  order by c_last_name
          ,c_first_name
          ,ca_city
          ,bought_city
          ,ss_ticket_number
  limit 100;

-- end query 1 in stream 0 using template query46.tpl
