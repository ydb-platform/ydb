{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query72.tpl and seed 2031708268
select  item.i_item_desc
      ,warehouse.w_warehouse_name
      ,d1.d_week_seq
      ,sum(case when p_promo_sk is null then 1 else 0 end) no_promo
      ,sum(case when p_promo_sk is not null then 1 else 0 end) promo
      ,count(*) total_cnt
from {{catalog_sales}} as catalog_sales
join {{customer_demographics}} as customer_demographics on (catalog_sales.cs_bill_cdemo_sk = customer_demographics.cd_demo_sk)
join {{household_demographics}} as household_demographics on (catalog_sales.cs_bill_hdemo_sk = household_demographics.hd_demo_sk)
join {{date_dim}} d1 on (catalog_sales.cs_sold_date_sk = d1.d_date_sk)
join {{inventory}} as inventory on (catalog_sales.cs_item_sk = inventory.inv_item_sk)
join {{date_dim}} d2 on (inventory.inv_date_sk = d2.d_date_sk)
join {{warehouse}} as warehouse on (warehouse.w_warehouse_sk=inventory.inv_warehouse_sk)
join {{item}} as item on (item.i_item_sk = catalog_sales.cs_item_sk)
join {{date_dim}} d3 on (catalog_sales.cs_ship_date_sk = d3.d_date_sk)
left join {{promotion}} as promotion on (catalog_sales.cs_promo_sk=promotion.p_promo_sk)
left join {{catalog_returns}} as catalog_returns on (catalog_returns.cr_item_sk = catalog_sales.cs_item_sk and catalog_returns.cr_order_number = catalog_sales.cs_order_number)
where d1.d_week_seq = d2.d_week_seq
  and inv_quantity_on_hand < cs_quantity
  and cast(d3.d_date as date) > cast(d1.d_date as date) + DateTime::IntervalFromDays(5)
  and hd_buy_potential = '1001-5000'
  and d1.d_year = 2000
  and cd_marital_status = 'D'
group by item.i_item_desc,warehouse.w_warehouse_name,d1.d_week_seq
order by total_cnt desc, item.i_item_desc, warehouse.w_warehouse_name, d1.d_week_seq
limit 100;

-- end query 1 in stream 0 using template query72.tpl
