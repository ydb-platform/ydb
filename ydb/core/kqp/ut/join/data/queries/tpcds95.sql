pragma TablePathPrefix = "/Root/test/ds/";
-- NB: Subquerys
$ws_wh =
(select ws1.ws_order_number ws_order_number,ws1.ws_warehouse_sk wh1,ws2.ws_warehouse_sk wh2
 from web_sales ws1 cross join web_sales ws2
 where ws1.ws_order_number = ws2.ws_order_number
   and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk);
-- start query 1 in stream 0 using template query95.tpl and seed 2031708268
 select
   count(distinct ws1.ws_order_number) as `order count`
  ,sum(ws_ext_ship_cost) as `total shipping cost`
  ,sum(ws_net_profit) as `total net profit`
from
   web_sales ws1
  cross join date_dim
  cross join customer_address
  cross join web_site
where
    cast(d_date as date) between cast('2002-4-01' as date) and
           (cast('2002-4-01' as date) + DateTime::IntervalFromDays(60))
and ws1.ws_ship_date_sk = d_date_sk
and ws1.ws_ship_addr_sk = ca_address_sk
and ca_state = 'AL'
and ws1.ws_web_site_sk = web_site_sk
and web_company_name = 'pri'
and ws1.ws_order_number in (select ws_order_number
                            from $ws_wh)
and ws1.ws_order_number in (select wr_order_number
                            from web_returns cross join $ws_wh ws_wh
                            where wr_order_number = ws_wh.ws_order_number)
order by `order count`
limit 100;
