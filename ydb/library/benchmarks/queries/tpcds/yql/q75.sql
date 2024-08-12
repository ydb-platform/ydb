{% include 'header.sql.jinja' %}

-- NB: Subquerys
$todecimal = ($x) -> {
  return cast(cast($x as string?) as decimal(7,2))
};

$sales_detail = (SELECT d_year
             ,i_brand_id
             ,i_class_id
             ,i_category_id
             ,i_manufact_id
             ,cs_quantity - COALESCE(cr_return_quantity,0) AS sales_cnt
             ,$todecimal(cs_ext_sales_price) - COALESCE($todecimal(cr_return_amount),cast(0 as decimal(7,2))) AS sales_amt
       FROM {{catalog_sales}} as catalog_sales 
       JOIN {{item}} as item ON item.i_item_sk=catalog_sales.cs_item_sk
                          JOIN {{date_dim}} as date_dim ON date_dim.d_date_sk=catalog_sales.cs_sold_date_sk
                          LEFT JOIN {{catalog_returns}} as catalog_returns ON (catalog_sales.cs_order_number=catalog_returns.cr_order_number
                                                    AND catalog_sales.cs_item_sk=catalog_returns.cr_item_sk)
       WHERE i_category='Sports'
       UNION all
       SELECT d_year
             ,i_brand_id
             ,i_class_id
             ,i_category_id
             ,i_manufact_id
             ,ss_quantity - COALESCE(sr_return_quantity,0) AS sales_cnt
             ,$todecimal(ss_ext_sales_price) - COALESCE($todecimal(sr_return_amt),cast(0 as decimal(7,2))) AS sales_amt
       FROM {{store_sales}} as store_sales
       JOIN {{item}} as item ON item.i_item_sk=store_sales.ss_item_sk
                        JOIN {{date_dim}} as date_dim ON date_dim.d_date_sk=store_sales.ss_sold_date_sk
                        LEFT JOIN {{store_returns}} as store_returns ON (store_sales.ss_ticket_number=store_returns.sr_ticket_number
                                                AND store_sales.ss_item_sk=store_returns.sr_item_sk)
       WHERE i_category='Sports'
       UNION all
       SELECT d_year
             ,i_brand_id
             ,i_class_id
             ,i_category_id
             ,i_manufact_id
             ,ws_quantity - COALESCE(wr_return_quantity,0) AS sales_cnt
             ,$todecimal(ws_ext_sales_price) - COALESCE($todecimal(wr_return_amt),cast(0 as decimal(7,2))) AS sales_amt
       FROM {{web_sales}} as web_sales 
       JOIN {{item}} as item ON item.i_item_sk=web_sales.ws_item_sk
                      JOIN {{date_dim}} as date_dim ON date_dim.d_date_sk=web_sales.ws_sold_date_sk
                      LEFT JOIN {{web_returns}} as web_returns ON (web_sales.ws_order_number=web_returns.wr_order_number
                                            AND web_sales.ws_item_sk=web_returns.wr_item_sk)
       WHERE i_category='Sports');

$all_sales = (
 SELECT d_year
       ,i_brand_id
       ,i_class_id
       ,i_category_id
       ,i_manufact_id
       ,SUM(sales_cnt) AS sales_cnt
       ,SUM(sales_amt) AS sales_amt
 FROM (select DISTINCT * from $sales_detail) sales_detail
 GROUP BY d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id);
-- start query 1 in stream 0 using template query75.tpl and seed 1819994127
 SELECT  prev_yr.d_year AS prev_year
                          ,curr_yr.d_year AS year
                          ,curr_yr.i_brand_id
                          ,curr_yr.i_class_id
                          ,curr_yr.i_category_id
                          ,curr_yr.i_manufact_id
                          ,prev_yr.sales_cnt AS prev_yr_cnt
                          ,curr_yr.sales_cnt AS curr_yr_cnt
                          ,curr_yr.sales_cnt-prev_yr.sales_cnt AS sales_cnt_diff
                          ,curr_yr.sales_amt-prev_yr.sales_amt AS sales_amt_diff
 FROM $all_sales curr_yr cross join $all_sales prev_yr
 WHERE curr_yr.i_brand_id=prev_yr.i_brand_id
   AND curr_yr.i_class_id=prev_yr.i_class_id
   AND curr_yr.i_category_id=prev_yr.i_category_id
   AND curr_yr.i_manufact_id=prev_yr.i_manufact_id
   AND curr_yr.d_year=2001
   AND prev_yr.d_year=2001-1
   AND cast((CAST(curr_yr.sales_cnt AS decimal(17, 2))/CAST(prev_yr.sales_cnt AS decimal(17, 2))) AS double)<0.9
 ORDER BY sales_cnt_diff,sales_amt_diff
 limit 100;

-- end query 1 in stream 0 using template query75.tpl
