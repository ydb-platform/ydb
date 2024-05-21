{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query17.tpl and seed 1819994127
$nantonull = ($n) -> {
    return case when Math::IsNaN($n)
        then null
        else $n
        end;
};

select  item.i_item_id
       ,item.i_item_desc
       ,store.s_state
       ,count(ss_quantity) as store_sales_quantitycount
       ,avg(ss_quantity) as store_sales_quantityave
       ,$nantonull(stddev_samp(ss_quantity)) as store_sales_quantitystdev
       ,$nantonull(stddev_samp(ss_quantity)/avg(ss_quantity)) as store_sales_quantitycov
       ,count(sr_return_quantity) as store_returns_quantitycount
       ,avg(sr_return_quantity) as store_returns_quantityave
       ,$nantonull(stddev_samp(sr_return_quantity)) as store_returns_quantitystdev
       ,$nantonull(stddev_samp(sr_return_quantity)/avg(sr_return_quantity)) as store_returns_quantitycov
       ,count(cs_quantity) as catalog_sales_quantitycount ,avg(cs_quantity) as catalog_sales_quantityave
       ,$nantonull(stddev_samp(cs_quantity)) as catalog_sales_quantitystdev
       ,$nantonull(stddev_samp(cs_quantity)/avg(cs_quantity)) as catalog_sales_quantitycov
 from {{store_sales}} as store_sales
     cross join {{store_returns}} as store_returns
     cross join {{catalog_sales}} as catalog_sales
     cross join {{date_dim}} d1
     cross join {{date_dim}} d2
     cross join {{date_dim}} d3
     cross join {{store}} as store
     cross join {{item}} as item
 where d1.d_quarter_name = '2000Q1'
   and d1.d_date_sk = ss_sold_date_sk
   and i_item_sk = ss_item_sk
   and s_store_sk = ss_store_sk
   and ss_customer_sk = sr_customer_sk
   and ss_item_sk = sr_item_sk
   and ss_ticket_number = sr_ticket_number
   and sr_returned_date_sk = d2.d_date_sk
   and d2.d_quarter_name in ('2000Q1','2000Q2','2000Q3')
   and sr_customer_sk = cs_bill_customer_sk
   and sr_item_sk = cs_item_sk
   and cs_sold_date_sk = d3.d_date_sk
   and d3.d_quarter_name in ('2000Q1','2000Q2','2000Q3')
 group by item.i_item_id
         ,item.i_item_desc
         ,store.s_state
 order by item.i_item_id
         ,item.i_item_desc
         ,store.s_state
limit 100;

-- end query 1 in stream 0 using template query17.tpl
