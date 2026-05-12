--!syntax_pg
--TPC-DS Q65

-- start query 1 in stream 0 using template ../query_templates/query65.tpl
select 
	s_store_name,
	i_item_desc,
	sc.revenue,
	i_current_price,
	i_wholesale_cost,
	i_brand
 from plato.store, plato.item,
     (select ss_store_sk, avg(revenue) as ave
 	from
 	    (select  ss_store_sk, ss_item_sk, 
 		     sum(ss_sales_price) as revenue
 		from plato.store_sales, plato.date_dim
 		where ss_sold_date_sk = d_date_sk and d_month_seq between 1212 and 1212+11
 		group by ss_store_sk, ss_item_sk) sa
 	group by ss_store_sk) sb,
     (select  ss_store_sk, ss_item_sk, sum(ss_sales_price) as revenue
 	from plato.store_sales, plato.date_dim
 	where ss_sold_date_sk = d_date_sk and d_month_seq between 1212 and 1212+11
 	group by ss_store_sk, ss_item_sk) sc
 where sb.ss_store_sk = sc.ss_store_sk and 
       sc.revenue <= 0.1::numeric * sb.ave and
       s_store_sk = sc.ss_store_sk and
       i_item_sk = sc.ss_item_sk
 order by s_store_name, i_item_desc
limit 100;

-- end query 1 in stream 0 using template ../query_templates/query65.tpl
