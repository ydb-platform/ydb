{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query70.tpl and seed 1819994127
select
    sum(ss_net_profit) as total_sum
   ,store.s_state
   ,store.s_county
   ,grouping(store.s_state)+grouping(store.s_county) as lochierarchy
   ,rank() over (
 	partition by grouping(store.s_state)+grouping(store.s_county),
 	case when grouping(store.s_county) = 0 then s_state else null end
 	order by sum(ss_net_profit) desc) as rank_within_parent
 from
    {{store_sales}} as store_sales
   cross join {{date_dim}}       d1
   cross join {{store}} as store
 where
    d1.d_month_seq between 1218 and 1218+11
 and d1.d_date_sk = ss_sold_date_sk
 and s_store_sk  = ss_store_sk
 and s_state in
             ( select s_state
               from  (select store.s_state as s_state,
 			    rank() over ( partition by store.s_state order by sum(ss_net_profit) desc) as ranking
                      from   {{store_sales}} as store_sales
                      cross join {{store}} as store
                      cross join {{date_dim}} as date_dim
                      where  d_month_seq between 1218 and 1218+11
 			    and d_date_sk = ss_sold_date_sk
 			    and s_store_sk  = ss_store_sk
                      group by store.s_state
                     ) tmp1
               where ranking <= 5
             )
 group by rollup(store.s_state,store.s_county)
 order by
   lochierarchy desc
  ,case when lochierarchy = 0 then s_state else null end
  ,rank_within_parent
 limit 100;

-- end query 1 in stream 0 using template query70.tpl
