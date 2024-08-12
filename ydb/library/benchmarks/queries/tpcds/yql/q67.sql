{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query67.tpl and seed 1819994127

$todecimal = ($x) -> {
  return cast(cast($x as string?) as decimal(7,2))
};

select  *
from (select i_category
            ,i_class
            ,i_brand
            ,i_product_name
            ,d_year
            ,d_qoy
            ,d_moy
            ,s_store_id
            ,sumsales
            ,rank() over (partition by i_category order by sumsales desc) rk
      from (select item.i_category i_category
                  ,item.i_class i_class
                  ,item.i_brand i_brand
                  ,item.i_product_name i_product_name
                  ,date_dim.d_year d_year
                  ,date_dim.d_qoy d_qoy
                  ,date_dim.d_moy d_moy
                  ,store.s_store_id s_store_id
                  ,sum(coalesce($todecimal(ss_sales_price)*ss_quantity, cast(0 as decimal(7,2)))) sumsales
            from {{store_sales}} as store_sales
                cross join {{date_dim}} as date_dim
                cross join {{store}} as store
                cross join {{item}} as item
       where  ss_sold_date_sk=d_date_sk
          and ss_item_sk=i_item_sk
          and ss_store_sk = s_store_sk
          and d_month_seq between 1185 and 1185+11
       group by  rollup(item.i_category, item.i_class, item.i_brand, item.i_product_name, date_dim.d_year, date_dim.d_qoy, date_dim.d_moy,store.s_store_id))dw1) dw2
where rk <= 100
order by i_category
        ,i_class
        ,i_brand
        ,i_product_name
        ,d_year
        ,d_qoy
        ,d_moy
        ,s_store_id
        ,sumsales
        ,rk
limit 100;

-- end query 1 in stream 0 using template query67.tpl
