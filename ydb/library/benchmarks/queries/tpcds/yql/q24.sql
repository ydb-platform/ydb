{% include 'header.sql.jinja' %}

-- NB: Subquerys
$ssales =
(select customer.c_last_name c_last_name
      ,customer.c_first_name c_first_name
      ,store.s_store_name s_store_name
      ,customer_address.ca_state ca_state
      ,store.s_state s_state
      ,item.i_color i_color
      ,item.i_current_price i_current_price
      ,item.i_manager_id i_manager_id
      ,item.i_units i_units
      ,item.i_size i_size
      ,sum(ss_sales_price) netpaid
from {{store_sales}} as store_sales
    cross join {{store_returns}} as store_returns
    cross join {{store}} as store
    cross join {{item}} as item
    cross join {{customer}} as customer
    cross join {{customer_address}} as customer_address
where ss_ticket_number = sr_ticket_number
  and ss_item_sk = sr_item_sk
  and ss_customer_sk = c_customer_sk
  and ss_item_sk = i_item_sk
  and ss_store_sk = s_store_sk
  and c_current_addr_sk = ca_address_sk
  and c_birth_country <> Unicode::ToUpper(Cast(ca_country as Utf8))
  and s_zip = ca_zip
and s_market_id=10
group by customer.c_last_name
        ,customer.c_first_name
        ,store.s_store_name
        ,customer_address.ca_state
        ,store.s_state
        ,item.i_color
        ,item.i_current_price
        ,item.i_manager_id
        ,item.i_units
        ,item.i_size);

$avg_netpaid = (select avg(netpaid) from $ssales);

-- start query 1 in stream 0 using template query24.tpl and seed 1220860970
select c_last_name
      ,c_first_name
      ,s_store_name
      ,sum(netpaid) paid
from $ssales
where i_color = 'snow'
group by c_last_name
        ,c_first_name
        ,s_store_name
having sum(netpaid) > $z0_05_35*$avg_netpaid
order by c_last_name
        ,c_first_name
        ,s_store_name
;

select c_last_name
      ,c_first_name
      ,s_store_name
      ,sum(netpaid) paid
from $ssales
where i_color = 'chiffon'
group by c_last_name
        ,c_first_name
        ,s_store_name
having sum(netpaid) > $z0_05_35*$avg_netpaid
order by c_last_name
        ,c_first_name
        ,s_store_name
;

-- end query 1 in stream 0 using template query24.tpl
