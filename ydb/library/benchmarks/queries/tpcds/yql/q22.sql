{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query22.tpl and seed 1819994127
select  item.i_product_name
             ,item.i_brand
             ,item.i_class
             ,item.i_category
             ,avg(inv_quantity_on_hand) qoh
       from {{inventory}} as inventory
           cross join {{date_dim}} as date_dim
           cross join {{item}} as item
       where inv_date_sk=d_date_sk
              and inv_item_sk=i_item_sk
              and d_month_seq between 1200 and 1200 + 11
       group by rollup(item.i_product_name
                       ,item.i_brand
                       ,item.i_class
                       ,item.i_category)
order by qoh, item.i_product_name, item.i_brand, item.i_class, item.i_category
limit 100;

-- end query 1 in stream 0 using template query22.tpl
