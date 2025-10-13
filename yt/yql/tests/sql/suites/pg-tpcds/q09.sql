--!syntax_pg
--TPC-DS Q9

-- start query 1 in stream 0 using template ../query_templates/query9.tpl
select case when (select count(*) 
                  from plato.store_sales 
                  where ss_quantity between 1 and 20) > 25437
            then (select avg(ss_ext_discount_amt) 
                  from plato.store_sales 
                  where ss_quantity between 1 and 20) 
            else (select avg(ss_net_profit)
                  from plato.store_sales
                  where ss_quantity between 1 and 20) end bucket1 ,
       case when (select count(*)
                  from plato.store_sales
                  where ss_quantity between 21 and 40) > 22746
            then (select avg(ss_ext_discount_amt)
                  from plato.store_sales
                  where ss_quantity between 21 and 40) 
            else (select avg(ss_net_profit)
                  from plato.store_sales
                  where ss_quantity between 21 and 40) end bucket2,
       case when (select count(*)
                  from plato.store_sales
                  where ss_quantity between 41 and 60) > 9387
            then (select avg(ss_ext_discount_amt)
                  from plato.store_sales
                  where ss_quantity between 41 and 60)
            else (select avg(ss_net_profit)
                  from plato.store_sales
                  where ss_quantity between 41 and 60) end bucket3,
       case when (select count(*)
                  from plato.store_sales
                  where ss_quantity between 61 and 80) > 10098
            then (select avg(ss_ext_discount_amt)
                  from plato.store_sales
                  where ss_quantity between 61 and 80)
            else (select avg(ss_net_profit)
                  from plato.store_sales
                  where ss_quantity between 61 and 80) end bucket4,
       case when (select count(*)
                  from plato.store_sales
                  where ss_quantity between 81 and 100) > 18213
            then (select avg(ss_ext_discount_amt)
                  from plato.store_sales
                  where ss_quantity between 81 and 100)
            else (select avg(ss_net_profit)
                  from plato.store_sales
                  where ss_quantity between 81 and 100) end bucket5
from plato.reason
where r_reason_sk = 1
;

-- end query 1 in stream 0 using template ../query_templates/query9.tpl
