--!syntax_pg
--TPC-DS Q6

-- start query 1 in stream 0 using template ../query_templates/query6.tpl
select  a.ca_state state, count(*) cnt
 from plato.customer_address a
     ,plato.customer c
     ,plato.store_sales s
     ,plato.date_dim d
     ,plato.item i
 where       a.ca_address_sk = c.c_current_addr_sk
 	and c.c_customer_sk = s.ss_customer_sk
 	and s.ss_sold_date_sk = d.d_date_sk
 	and s.ss_item_sk = i.i_item_sk
 	and d.d_month_seq = 
 	     (select distinct (d_month_seq)
 	      from plato.date_dim
               where d_year = 2000
 	        and d_moy = 2 )
 	and i.i_current_price > 1.2::numeric * 
             (select avg(j.i_current_price) 
 	     from plato.item j 
 	     where j.i_category = i.i_category)
 group by a.ca_state
 having count(*) >= 10
 order by cnt, a.ca_state 
 limit 100;

-- end query 1 in stream 0 using template ../query_templates/query6.tpl
