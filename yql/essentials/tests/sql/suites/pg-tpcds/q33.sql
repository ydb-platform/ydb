--!syntax_pg
--TPC-DS Q33

-- start query 1 in stream 0 using template ../query_templates/query33.tpl
with ss as (
 select
          i_manufact_id,sum(ss_ext_sales_price) total_sales
 from
 	plato.store_sales,
 	plato.date_dim,
         plato.customer_address,
         plato.item
 where
         i_manufact_id in (select
  i_manufact_id
from
 plato.item
where i_category in ('Books'))
 and     ss_item_sk              = i_item_sk
 and     ss_sold_date_sk         = d_date_sk
 and     d_year                  = 1999
 and     d_moy                   = 3
 and     ss_addr_sk              = ca_address_sk
 and     ca_gmt_offset           = -5::numeric
 group by i_manufact_id),
 cs as (
 select
          i_manufact_id,sum(cs_ext_sales_price) total_sales
 from
 	plato.catalog_sales,
 	plato.date_dim,
         plato.customer_address,
         plato.item
 where
         i_manufact_id               in (select
  i_manufact_id
from
 plato.item
where i_category in ('Books'))
 and     cs_item_sk              = i_item_sk
 and     cs_sold_date_sk         = d_date_sk
 and     d_year                  = 1999
 and     d_moy                   = 3
 and     cs_bill_addr_sk         = ca_address_sk
 and     ca_gmt_offset           = -5::numeric
 group by i_manufact_id),
 ws as (
 select
          i_manufact_id,sum(ws_ext_sales_price) total_sales
 from
 	plato.web_sales,
 	plato.date_dim,
         plato.customer_address,
         plato.item
 where
         i_manufact_id               in (select
  i_manufact_id
from
 plato.item
where i_category in ('Books'))
 and     ws_item_sk              = i_item_sk
 and     ws_sold_date_sk         = d_date_sk
 and     d_year                  = 1999
 and     d_moy                   = 3
 and     ws_bill_addr_sk         = ca_address_sk
 and     ca_gmt_offset           = -5::numeric
 group by i_manufact_id)
  select  i_manufact_id ,sum(total_sales) total_sales
 from  (select * from ss 
        union all
        select * from cs 
        union all
        select * from ws) tmp1
 group by i_manufact_id
 order by total_sales
limit 100;

-- end query 1 in stream 0 using template ../query_templates/query33.tpl
