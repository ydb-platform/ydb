{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query89.tpl and seed 1719819282

$todecimal = ($x) -> {
  return cast(cast($x as string?) as decimal(7,2))
};

select  *
from(
select item.i_category, item.i_class, item.i_brand,
       store.s_store_name s_store_name, store.s_company_name,
       date_dim.d_moy,
       sum($todecimal(ss_sales_price)) sum_sales,
       avg(sum($todecimal(ss_sales_price))) over
         (partition by item.i_category, item.i_brand, store.s_store_name, store.s_company_name)
         avg_monthly_sales
from {{item}} as item
cross join {{store_sales}} as store_sales
cross join {{date_dim}} as date_dim 
cross join {{store}} as store
where ss_item_sk = i_item_sk and
      ss_sold_date_sk = d_date_sk and
      ss_store_sk = s_store_sk and
      d_year in (2000) and
        ((i_category in ('Home','Music','Books') and
          i_class in ('glassware','classical','fiction')
         )
      or (i_category in ('Jewelry','Sports','Women') and
          i_class in ('semi-precious','baseball','dresses')
        ))
group by item.i_category, item.i_class, item.i_brand,
         store.s_store_name, store.s_company_name, date_dim.d_moy) tmp1
where case when (avg_monthly_sales <> cast(0 as decimal(7,2))) then (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales) else null end > cast("0.1" as decimal(7,2))
order by sum_sales - avg_monthly_sales, s_store_name
limit 100;

-- end query 1 in stream 0 using template query89.tpl
