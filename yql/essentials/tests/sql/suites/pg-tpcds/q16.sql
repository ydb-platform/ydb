--!syntax_pg
--TPC-DS Q16

-- start query 1 in stream 0 using template ../query_templates/query16.tpl
select  
   count(distinct cs_order_number) as "order count"
  ,sum(cs_ext_ship_cost) as "total shipping cost"
  ,sum(cs_net_profit) as "total net profit"
from
   plato.catalog_sales cs1
  ,plato.date_dim
  ,plato.customer_address
  ,plato.call_center
where
    d_date between '1999-2-01'::date and 
           (cast('1999-2-01' as date) + interval '60' day)::date
and cs1.cs_ship_date_sk = d_date_sk
and cs1.cs_ship_addr_sk = ca_address_sk
and ca_state = 'IL'
and cs1.cs_call_center_sk = cc_call_center_sk
and cc_county in ('Williamson County','Williamson County','Williamson County','Williamson County',
                  'Williamson County'
)
and exists (select *
            from plato.catalog_sales cs2
            where cs1.cs_order_number = cs2.cs_order_number
              and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
and not exists(select *
               from plato.catalog_returns cr1
               where cs1.cs_order_number = cr1.cr_order_number)
order by count(distinct cs_order_number)
limit 100;

-- end query 1 in stream 0 using template ../query_templates/query16.tpl
