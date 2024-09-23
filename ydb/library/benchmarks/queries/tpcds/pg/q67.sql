{% include 'header.sql.jinja' %}

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
      from (select i_category
                  ,i_class
                  ,i_brand
                  ,i_product_name
                  ,d_year
                  ,d_qoy
                  ,d_moy
                  ,s_store_id
                  ,sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales
            from {{store_sales}}
                ,{{date_dim}}
                ,{{store}}
                ,{{item}}
       where  ss_sold_date_sk=d_date_sk
          and ss_item_sk=i_item_sk
          and ss_store_sk = s_store_sk
          and d_month_seq between 1185 and 1185+11
       group by  rollup(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,s_store_id))dw1) dw2
where rk <= 100
order by i_category nulls first
        ,i_class nulls first
        ,i_brand nulls first
        ,i_product_name nulls first
        ,d_year nulls first
        ,d_qoy nulls first
        ,d_moy nulls first
        ,s_store_id nulls first
        ,sumsales nulls first
        ,rk nulls first
limit 100;

-- end query 1 in stream 0 using template ../query_templates/query67.tpl
