pragma TablePathPrefix = "/Root/test/ds/";

-- NB: Subquerys
$bla1 = (select ws_order_number
            from web_sales
              group by ws_order_number
              having COUNT(DISTINCT ws_warehouse_sk) > 1);

-- start query 1 in stream 0 using template query94.tpl and seed 2031708268
select
   count(distinct ws1.ws_order_number) as `order count`
  ,sum(ws_ext_ship_cost) as `total shipping cost`
  ,sum(ws_net_profit) as `total net profit`
from
   web_sales ws1
  cross join date_dim
  cross join customer_address
  cross join web_site
  left semi join $bla1 bla1 on (ws1.ws_order_number = bla1.ws_order_number)
  left only join web_returns on (ws1.ws_order_number = web_returns.wr_order_number)
where
    cast(d_date as date) between cast('1999-4-01' as date) and
           (cast('1999-4-01' as date) + DateTime::IntervalFromDays(60))
and ws1.ws_ship_date_sk = d_date_sk
and ws1.ws_ship_addr_sk = ca_address_sk
and ca_state = 'NE'
and ws1.ws_web_site_sk = web_site_sk
and web_company_name = 'pri'
order by `order count`
limit 100;
