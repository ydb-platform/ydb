{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query49.tpl and seed 1819994127

$todecimal = ($x) -> {
  return cast(cast($x as string?) as decimal(15,4))
};

$non_unique = (select  channel, item, return_ratio, return_rank, currency_rank from
 (select
 'web' as channel
 ,web.item as item
 ,web.return_ratio as return_ratio
 ,web.return_rank as return_rank
 ,web.currency_rank as currency_rank
 from (
 	select
 	 item
 	,return_ratio
 	,currency_ratio
 	,rank() over (order by return_ratio) as return_rank
 	,rank() over (order by currency_ratio) as currency_rank
 	from
 	(	select ws.ws_item_sk as item
 		,(sum(coalesce($todecimal(wr.wr_return_quantity),cast(0 as decimal(15,4))))/
 		sum(coalesce($todecimal(ws.ws_quantity),cast(0 as decimal(15,4))))) as return_ratio
 		,(sum(coalesce($todecimal(wr.wr_return_amt),cast(0 as decimal(15,4))))/
 		sum(coalesce($todecimal(ws.ws_net_paid),cast(0 as decimal(15,4))))) as currency_ratio
 		from
 		 {{web_sales}} ws
         left join {{web_returns}} wr
 			on (ws.ws_order_number = wr.wr_order_number and
 			ws.ws_item_sk = wr.wr_item_sk)
                 cross join {{date_dim}} as date_dim
 		where
 			wr.wr_return_amt > 10000
 			and ws.ws_net_profit > 1
                         and ws.ws_net_paid > 0
                         and ws.ws_quantity > 0
                         and ws_sold_date_sk = d_date_sk
                         and d_year = 2000
                         and d_moy = 12
 		group by ws.ws_item_sk
 	) in_web
 ) web
 where
 (
 web.return_rank <= 10
 or
 web.currency_rank <= 10
 )
 union all
 select
 'catalog' as channel
 ,catalog.item as item
 ,catalog.return_ratio as return_ratio
 ,catalog.return_rank as return_rank
 ,catalog.currency_rank as currency_rank
 from (
 	select
 	 item
 	,return_ratio
 	,currency_ratio
 	,rank() over (order by return_ratio) as return_rank
 	,rank() over (order by currency_ratio) as currency_rank
 	from
 	(	select
 		cs.cs_item_sk as item
 		,(sum(coalesce($todecimal(cr.cr_return_quantity),cast(0 as decimal(15,4))))/
 		sum(coalesce($todecimal(cs.cs_quantity),cast(0 as decimal(15,4))))) as return_ratio
 		,(sum(coalesce($todecimal(cr.cr_return_amount),cast(0 as decimal(15,4))))/
 		sum(coalesce($todecimal(cs.cs_net_paid),cast(0 as decimal(15,4))))) as currency_ratio
 		from
 		{{catalog_sales}} cs
        left outer join {{catalog_returns}} cr
 			on (cs.cs_order_number = cr.cr_order_number and
 			cs.cs_item_sk = cr.cr_item_sk)
                cross join {{date_dim}} as date_dim
 		where
 			cr.cr_return_amount > 10000
 			and cs.cs_net_profit > 1
                         and cs.cs_net_paid > 0
                         and cs.cs_quantity > 0
                         and cs_sold_date_sk = d_date_sk
                         and d_year = 2000
                         and d_moy = 12
                 group by cs.cs_item_sk
 	) in_cat
 ) catalog
 where
 (
 catalog.return_rank <= 10
 or
 catalog.currency_rank <=10
 )
 union all
 select
 'store' as channel
 ,store.item as item
 ,store.return_ratio as return_ratio
 ,store.return_rank as return_rank
 ,store.currency_rank as currency_rank
 from (
 	select
 	 item
 	,return_ratio
 	,currency_ratio
 	,rank() over (order by return_ratio) as return_rank
 	,rank() over (order by currency_ratio) as currency_rank
 	from
 	(	select sts.ss_item_sk as item
 		,(sum(coalesce($todecimal(sr.sr_return_quantity),cast(0 as decimal(15,4))))/
        sum(coalesce($todecimal(sts.ss_quantity),cast(0 as decimal(15,4))))) as return_ratio
 		,(sum(coalesce($todecimal(sr.sr_return_amt),cast(0 as decimal(15,4))))/
        sum(coalesce($todecimal(sts.ss_net_paid),cast(0 as decimal(15,4))))) as currency_ratio
 		from
 		{{store_sales}} sts
        left outer join {{store_returns}} sr
 			on (sts.ss_ticket_number = sr.sr_ticket_number and sts.ss_item_sk = sr.sr_item_sk)
                cross join {{date_dim}} as date_dim
 		where
 			sr.sr_return_amt > 10000
 			and sts.ss_net_profit > 1
                         and sts.ss_net_paid > 0
                         and sts.ss_quantity > 0
                         and ss_sold_date_sk = d_date_sk
                         and d_year = 2000
                         and d_moy = 12
 		group by sts.ss_item_sk
 	) in_store
 ) store
 where  (
 store.return_rank <= 10
 or
 store.currency_rank <= 10
 )
 ) y);

select DISTINCT *
 from $non_unique
 order by channel,return_rank,currency_rank,item
 limit 100;

--  'store' as channel
--  ,store.item as item
--  ,store.return_ratio as return_ratio
--  ,store.return_rank as return_rank
--  ,store.currency_rank as currency_rank

-- end query 1 in stream 0 using template query49.tpl
