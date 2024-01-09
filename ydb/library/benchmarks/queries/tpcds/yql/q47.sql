{% include 'header.sql.jinja' %}

-- NB: Subquerys
$v1 = (
 select item.i_category i_category, item.i_brand i_brand,
        store.s_store_name s_store_name, store.s_company_name s_company_name,
        date_dim.d_year d_year, date_dim.d_moy d_moy,
        sum(ss_sales_price) sum_sales,
        avg(sum(ss_sales_price)) over
          (partition by item.i_category, item.i_brand,
                     store.s_store_name, store.s_company_name, date_dim.d_year)
          avg_monthly_sales,
        rank() over
          (partition by item.i_category, item.i_brand,
                     store.s_store_name, store.s_company_name
           order by date_dim.d_year, date_dim.d_moy) rn
 from {{item}} as item
 cross join {{store_sales}} as store_sales
 cross join {{date_dim}} as date_dim
 cross join {{store}} as store
 where ss_item_sk = i_item_sk and
       ss_sold_date_sk = d_date_sk and
       ss_store_sk = s_store_sk and
       (
         d_year = 1999 or
         ( d_year = 1999-1 and d_moy =12) or
         ( d_year = 1999+1 and d_moy =1)
       )
 group by item.i_category, item.i_brand,
          store.s_store_name, store.s_company_name,
          date_dim.d_year, date_dim.d_moy);

$v2 = (
 select v1.s_store_name s_store_name
        ,v1.d_year d_year, v1.d_moy d_moy
        ,v1.avg_monthly_sales avg_monthly_sales
        ,v1.sum_sales sum_sales, v1_lag.sum_sales psum, v1_lead.sum_sales nsum
 from $v1 v1 cross join $v1 v1_lag cross join $v1 v1_lead
 where v1.i_category = v1_lag.i_category and
       v1.i_category = v1_lead.i_category and
       v1.i_brand = v1_lag.i_brand and
       v1.i_brand = v1_lead.i_brand and
       v1.s_store_name = v1_lag.s_store_name and
       v1.s_store_name = v1_lead.s_store_name and
       v1.s_company_name = v1_lag.s_company_name and
       v1.s_company_name = v1_lead.s_company_name and
       v1.rn = v1_lag.rn + 1 and
       v1.rn = v1_lead.rn - 1);


-- start query 1 in stream 0 using template query47.tpl and seed 2031708268
select *
 from $v2
 where  d_year = 1999 and
        avg_monthly_sales > 0 and
        case when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1
 order by sum_sales - avg_monthly_sales, sum_sales
 limit 100;

-- end query 1 in stream 0 using template query47.tpl
