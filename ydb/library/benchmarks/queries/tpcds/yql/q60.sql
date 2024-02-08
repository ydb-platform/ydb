{% include 'header.sql.jinja' %}

-- NB: Subquerys
$ss = (
 select
          item.i_item_id i_item_id,sum(ss_ext_sales_price) total_sales
 from
 	{{store_sales}} as store_sales cross join
 	{{date_dim}} as date_dim cross join
         {{customer_address}} as customer_address cross join
         {{item}} as item
 where
         i_item_id in (select
  i_item_id
from
 {{item}} as item
where i_category in ('Children'))
 and     ss_item_sk              = i_item_sk
 and     ss_sold_date_sk         = d_date_sk
 and     d_year                  = 1998
 and     d_moy                   = 10
 and     ss_addr_sk              = ca_address_sk
 and     ca_gmt_offset           = -5
 group by item.i_item_id);
$cs = (
 select
          item.i_item_id i_item_id,sum(cs_ext_sales_price) total_sales
 from
 	{{catalog_sales}} as catalog_sales cross join
 	{{date_dim}} as date_dim cross join
         {{customer_address}} as customer_address cross join
         {{item}} as item
 where
         i_item_id               in (select
  i_item_id
from
 {{item}} as item
where i_category in ('Children'))
 and     cs_item_sk              = i_item_sk
 and     cs_sold_date_sk         = d_date_sk
 and     d_year                  = 1998
 and     d_moy                   = 10
 and     cs_bill_addr_sk         = ca_address_sk
 and     ca_gmt_offset           = -5
 group by item.i_item_id);
 $ws = (
 select
          item.i_item_id i_item_id,sum(ws_ext_sales_price) total_sales
 from
 	{{web_sales}} as web_sales cross join
 	{{date_dim}} as date_dim cross join
         {{customer_address}} as customer_address cross join
         {{item}} as item
 where
         i_item_id               in (select
  i_item_id
from
 {{item}} as item
where i_category in ('Children'))
 and     ws_item_sk              = i_item_sk
 and     ws_sold_date_sk         = d_date_sk
 and     d_year                  = 1998
 and     d_moy                   = 10
 and     ws_bill_addr_sk         = ca_address_sk
 and     ca_gmt_offset           = -5
 group by item.i_item_id);
-- start query 1 in stream 0 using template query60.tpl and seed 1930872976
  select
  i_item_id
,sum(total_sales) total_sales
 from  (select * from $ss
        union all
        select * from $cs
        union all
        select * from $ws) tmp1
 group by i_item_id
 order by i_item_id
      ,total_sales
 limit 100;

-- end query 1 in stream 0 using template query60.tpl
