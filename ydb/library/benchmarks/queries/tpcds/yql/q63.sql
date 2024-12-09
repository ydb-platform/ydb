{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query63.tpl and seed 1819994127

select  *
from (select item.i_manager_id i_manager_id
             ,sum($todecimal(ss_sales_price, 7, 2)) sum_sales
             ,avg(sum($todecimal(ss_sales_price, 7, 2))) over (partition by item.i_manager_id) avg_monthly_sales
      from {{item}} as item
          cross join {{store_sales}} as store_sales
          cross join {{date_dim}} as date_dim
          cross join {{store}} as store
      where ss_item_sk = i_item_sk
        and ss_sold_date_sk = d_date_sk
        and ss_store_sk = s_store_sk
        and d_month_seq in (1200,1200+1,1200+2,1200+3,1200+4,1200+5,1200+6,1200+7,1200+8,1200+9,1200+10,1200+11)
        and ((    i_category in ('Books','Children','Electronics')
              and i_class in ('personal','portable','reference','self-help')
              and i_brand in ('scholaramalgamalg #14','scholaramalgamalg #7',
		                  'exportiunivamalg #9','scholaramalgamalg #9'))
           or(    i_category in ('Women','Music','Men')
              and i_class in ('accessories','classical','fragrances','pants')
              and i_brand in ('amalgimporto #1','edu packscholar #1','exportiimporto #1',
		                 'importoamalg #1')))
group by item.i_manager_id, date_dim.d_moy) tmp1
where case when avg_monthly_sales > 0 then abs (sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > $todecimal(0.1,7,2)
order by i_manager_id
        ,avg_monthly_sales
        ,sum_sales
limit 100;

-- end query 1 in stream 0 using template query63.tpl
