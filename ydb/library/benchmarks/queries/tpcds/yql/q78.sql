{% include 'header.sql.jinja' %}

-- NB: Subquerys
$ws =
  (select date_dim.d_year AS ws_sold_year, web_sales.ws_item_sk ws_item_sk,
    web_sales.ws_bill_customer_sk ws_customer_sk,
    sum(ws_quantity) ws_qty,
    sum(ws_wholesale_cost) ws_wc,
    sum(ws_sales_price) ws_sp
   from {{web_sales}} as web_sales
   left join {{web_returns}} as web_returns on web_returns.wr_order_number=web_sales.ws_order_number and web_sales.ws_item_sk=web_returns.wr_item_sk
   join {{date_dim}} as date_dim on web_sales.ws_sold_date_sk = date_dim.d_date_sk
   where wr_order_number is null
   group by date_dim.d_year, web_sales.ws_item_sk, web_sales.ws_bill_customer_sk
   );
$cs =
  (select date_dim.d_year AS cs_sold_year, catalog_sales.cs_item_sk cs_item_sk,
    catalog_sales.cs_bill_customer_sk cs_customer_sk,
    sum(cs_quantity) cs_qty,
    sum(cs_wholesale_cost) cs_wc,
    sum(cs_sales_price) cs_sp
   from {{catalog_sales}} as catalog_sales
   left join {{catalog_returns}} as catalog_returns on catalog_returns.cr_order_number=catalog_sales.cs_order_number and catalog_sales.cs_item_sk=catalog_returns.cr_item_sk
   join {{date_dim}} as date_dim on catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
   where cr_order_number is null
   group by date_dim.d_year, catalog_sales.cs_item_sk, catalog_sales.cs_bill_customer_sk
   );
$ss=
  (select date_dim.d_year AS ss_sold_year, store_sales.ss_item_sk ss_item_sk,
    store_sales.ss_customer_sk ss_customer_sk,
    sum(ss_quantity) ss_qty,
    sum(ss_wholesale_cost) ss_wc,
    sum(ss_sales_price) ss_sp
   from {{store_sales}} as store_sales
   left join {{store_returns}} as store_returns on store_returns.sr_ticket_number=store_sales.ss_ticket_number and store_sales.ss_item_sk=store_returns.sr_item_sk
   join {{date_dim}} as date_dim on store_sales.ss_sold_date_sk = date_dim.d_date_sk
   where sr_ticket_number is null
   group by date_dim.d_year, store_sales.ss_item_sk, store_sales.ss_customer_sk
   );
-- start query 1 in stream 0 using template query78.tpl and seed 1819994127
 select
ss_sold_year, ss_item_sk, ss_customer_sk,
Math::Round(cast(ss_qty as double)/(coalesce(ws_qty,0)+coalesce(cs_qty,0)),-2) ratio,
ss_qty store_qty, ss_wc store_wholesale_cost, ss_sp store_sales_price,
coalesce(ws_qty,0)+coalesce(cs_qty,0) other_chan_qty,
coalesce(ws_wc,$z0_35)+coalesce(cs_wc,$z0_35) other_chan_wholesale_cost,
coalesce(ws_sp,$z0_35)+coalesce(cs_sp,$z0_35) other_chan_sales_price
from $ss ss
left join $ws ws on (ws.ws_sold_year=ss.ss_sold_year and ws.ws_item_sk=ss.ss_item_sk and ws.ws_customer_sk=ss.ss_customer_sk)
left join $cs cs on (cs.cs_sold_year=ss.ss_sold_year and cs.cs_item_sk=ss.ss_item_sk and cs.cs_customer_sk=ss.ss_customer_sk)
where (coalesce(ws_qty,0)>0 or coalesce(cs_qty, 0)>0) and ss_sold_year=2001
order by
  ss_sold_year, ss_item_sk, ss_customer_sk,
  store_qty desc, store_wholesale_cost desc, store_sales_price desc,
  other_chan_qty,
  other_chan_wholesale_cost,
  other_chan_sales_price,
  ratio
limit 100;

-- end query 1 in stream 0 using template query78.tpl
