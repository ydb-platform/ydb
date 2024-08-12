{% include 'header.sql.jinja' %}

-- NB: Subquerys
$todecimal = ($x) -> {
  return cast(cast($x as string?) as decimal(7,2))
};

$v1 = (
 select item.i_category i_category, item.i_brand i_brand,
        call_center.cc_name cc_name,
        date_dim.d_year d_year, date_dim.d_moy d_moy,
        sum($todecimal(cs_sales_price)) sum_sales,
        avg(sum($todecimal(cs_sales_price))) over
          (partition by item.i_category, item.i_brand,
                     call_center.cc_name, date_dim.d_year)
          avg_monthly_sales,
        rank() over
          (partition by item.i_category, item.i_brand,
                     call_center.cc_name
           order by date_dim.d_year, date_dim.d_moy) rn
 from {{item}} as item
 cross join {{catalog_sales}} as catalog_sales
 cross join  {{date_dim}} as date_dim
 cross join {{call_center}} as call_center
 where cs_item_sk = i_item_sk and
       cs_sold_date_sk = d_date_sk and
       cc_call_center_sk= cs_call_center_sk and
       (
         d_year = 1999 or
         ( d_year = 1999-1 and d_moy =12) or
         ( d_year = 1999+1 and d_moy =1)
       )
 group by item.i_category, item.i_brand,
          call_center.cc_name , date_dim.d_year, date_dim.d_moy);
$v2 = (
 select v1.i_category i_category, v1.i_brand i_brand
        ,v1.d_year d_year, v1.d_moy d_moy
        ,v1.avg_monthly_sales avg_monthly_sales
        ,v1.sum_sales sum_sales, v1_lag.sum_sales psum, v1_lead.sum_sales nsum
 from $v1 v1 cross join $v1 v1_lag cross join $v1 v1_lead
 where v1.i_category = v1_lag.i_category and
       v1.i_category = v1_lead.i_category and
       v1.i_brand = v1_lag.i_brand and
       v1.i_brand = v1_lead.i_brand and
       v1. cc_name = v1_lag. cc_name and
       v1. cc_name = v1_lead. cc_name and
       v1.rn = v1_lag.rn + 1U and
       v1.rn = v1_lead.rn - 1U);
-- start query 1 in stream 0 using template query57.tpl and seed 2031708268
  select  *
 from $v2
 where  d_year = 1999 and
        avg_monthly_sales > 0 and
        case when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > cast("0.1" as decimal(7,2))
 order by sum_sales - avg_monthly_sales, avg_monthly_sales
 limit 100;

-- end query 1 in stream 0 using template query57.tpl
