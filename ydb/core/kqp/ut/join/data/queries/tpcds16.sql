-- NB: Subquerys
$orders_with_several_warehouses = (
    select cs_order_number
    from `/Root/test/ds/catalog_sales`
    group by cs_order_number
    having count(distinct cs_warehouse_sk) > 1
);

-- start query 1 in stream 0 using template query16.tpl and seed 171719422
select
   count(distinct cs1.cs_order_number) as `order count`
  ,sum(cs_ext_ship_cost) as `total shipping cost`
  ,sum(cs_net_profit) as `total net profit`
from
   `/Root/test/ds/catalog_sales` cs1
  cross join `/Root/test/ds/date_dim`
  cross join `/Root/test/ds/customer_address`
  cross join `/Root/test/ds/call_center`
  left semi join $orders_with_several_warehouses cs2 on cs1.cs_order_number = cs2.cs_order_number
  left only join `/Root/test/ds/catalog_returns` cr1 on cs1.cs_order_number = cr1.cr_order_number
where
    cast(d_date as date) between cast('1999-4-01' as date) and
           (cast('1999-4-01' as date) + DateTime::IntervalFromDays(60))
and cs1.cs_ship_date_sk = d_date_sk
and cs1.cs_ship_addr_sk = ca_address_sk
and ca_state = 'IL'
and cs1.cs_call_center_sk = cc_call_center_sk
and cc_county in ('Richland County','Bronx County','Maverick County','Mesa County',
                  'Raleigh County'
)
order by `order count`
limit 100;
