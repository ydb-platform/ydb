{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query34.tpl and seed 1971067816
select c_last_name
       ,c_first_name
       ,c_salutation
       ,c_preferred_cust_flag
       ,ss_ticket_number
       ,cnt from
   (select store_sales.ss_ticket_number ss_ticket_number
          ,store_sales.ss_customer_sk ss_customer_sk
          ,count(*) cnt
    from {{store_sales}} as store_sales 
    cross join {{date_dim}} as date_dim
    cross join {{store}} as store
    cross join {{household_demographics}} as household_demographics
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and (date_dim.d_dom between 1 and 3 or date_dim.d_dom between 25 and 28)
    and (household_demographics.hd_buy_potential = '>10000' or
         household_demographics.hd_buy_potential = 'Unknown')
    and household_demographics.hd_vehicle_count > 0
    and household_demographics.hd_dep_count / household_demographics.hd_vehicle_count > 1.2
    and date_dim.d_year in (2000,2000+1,2000+2)
    and store.s_county in ('Salem County','Terrell County','Arthur County','Oglethorpe County',
                           'Lunenburg County','Perry County','Halifax County','Sumner County')
    group by store_sales.ss_ticket_number,store_sales.ss_customer_sk) dn 
    cross join {{customer}} as customer
    where ss_customer_sk = c_customer_sk
      and cnt between 15 and 20
    order by c_last_name,c_first_name,c_salutation,c_preferred_cust_flag desc, ss_ticket_number;

-- end query 1 in stream 0 using template query34.tpl
