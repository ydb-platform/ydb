{% include 'header.sql.jinja' %}

$ss = (
 select item.i_item_id as i_item_id,sum(ss_ext_sales_price) total_sales
 from
    {{store_sales}} as store_sales
    cross join {{date_dim}} as date_dim
    cross join {{customer_address}} as customer_address
    cross join {{item}} as item
 where i_item_id in (select
     i_item_id
from {{item}} as item
where item.i_color in ('orchid','chiffon','lace'))
 and     ss_item_sk              = item.i_item_sk
 and     ss_sold_date_sk         = date_dim.d_date_sk
 and     date_dim.d_year                  = 2000
 and     date_dim.d_moy                   = 1
 and     ss_addr_sk              = customer_address.ca_address_sk
 and     customer_address.ca_gmt_offset           = -8
 group by item.i_item_id);

$cs = (
 select item.i_item_id as i_item_id,sum(cs_ext_sales_price) total_sales
 from
    {{catalog_sales}} as catalog_sales
    cross join {{date_dim}} as date_dim
    cross join {{customer_address}} as customer_address
    cross join {{item}} as item
 where
         item.i_item_id               in (select
  i_item_id
from {{item}} as item
where item.i_color in ('orchid','chiffon','lace'))
 and     cs_item_sk              = item.i_item_sk
 and     cs_sold_date_sk         = date_dim.d_date_sk
 and     date_dim.d_year                  = 2000
 and     date_dim.d_moy                   = 1
 and     cs_bill_addr_sk         = customer_address.ca_address_sk
 and     customer_address.ca_gmt_offset           = -8
 group by item.i_item_id);

$ws = (
 select item.i_item_id as i_item_id,sum(ws_ext_sales_price) total_sales
 from
    {{web_sales}} as web_sales
    cross join {{date_dim}} as date_dim
    cross join {{customer_address}} as customer_address
    cross join {{item}} as item
 where
         item.i_item_id               in (select
  i_item_id
from {{item}} as item
where i_color in ('orchid','chiffon','lace'))
 and     ws_item_sk              = item.i_item_sk
 and     ws_sold_date_sk         = date_dim.d_date_sk
 and     date_dim.d_year                  = 2000
 and     date_dim.d_moy                   = 1
 and     ws_bill_addr_sk         = customer_address.ca_address_sk
 and     customer_address.ca_gmt_offset           = -8
 group by item.i_item_id);

  select  i_item_id ,sum(total_sales) total_sales
 from  (select * from $ss
        union all
        select * from $cs
        union all
        select * from $ws) tmp1
 group by i_item_id
 order by total_sales,
          i_item_id
 limit 100;

-- end query 1 in stream 0 using template ../query_templates/query56.tpl
