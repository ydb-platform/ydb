{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query89.tpl and seed 1719819282

select  *
from(
select item.i_category, item.i_class, item.i_brand,
       store.s_store_name s_store_name, store.s_company_name,
       date_dim.d_moy,
       sum(ss_sales_price) sum_sales,
       avg(sum(ss_sales_price)) over
         (partition by item.i_category, item.i_brand, store.s_store_name, store.s_company_name)
         avg_monthly_sales
from {{item}} as item
cross join {{store_sales}} as store_sales
cross join {{date_dim}} as date_dim 
cross join {{store}} as store
where ss_item_sk = i_item_sk and
      ss_sold_date_sk = d_date_sk and
      ss_store_sk = s_store_sk and
      d_year in (1999) and
        ((i_category in ('Books','Electronics','Sports') and
          i_class in ('computers','stereo','football')
         )
      or (i_category in ('Men','Jewelry','Women') and
          i_class in ('shirts','birdal','dresses')
        ))
group by item.i_category, item.i_class, item.i_brand,
         store.s_store_name, store.s_company_name, date_dim.d_moy) tmp1
where case when (avg_monthly_sales <> $todecimal(0,'7','2')) then (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales) else null end > $todecimal(0.1,'7','2')
order by sum_sales - avg_monthly_sales, s_store_name
limit 100;

-- end query 1 in stream 0 using template query89.tpl
