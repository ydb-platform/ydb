--!syntax_pg
--TPC-DS Q37

-- start query 1 in stream 0 using template ../query_templates/query37.tpl
select  i_item_id
       ,i_item_desc
       ,i_current_price
 from plato.item, plato.inventory, plato.date_dim, plato.catalog_sales
 where i_current_price between 22::numeric and (22 + 30)::numeric
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and d_date between cast('2001-06-02' as date) and (cast('2001-06-02' as date) + interval '60' day)::date
 and i_manufact_id in (678,964,918,849)
 and inv_quantity_on_hand between 100 and 500
 and cs_item_sk = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
 limit 100;

-- end query 1 in stream 0 using template ../query_templates/query37.tpl
