{% include 'header.sql.jinja' %}

-- NB: Subquerys
$bla1 = (select iss.i_brand_id brand_id
     ,iss.i_class_id class_id
     ,iss.i_category_id category_id
 from {{store_sales}} as store_sales
     cross join {{item}} iss
     cross join {{date_dim}} d1
 where ss_item_sk = iss.i_item_sk
   and ss_sold_date_sk = d1.d_date_sk
   and d1.d_year between 2000 AND 2000 + 2);
$bla2 = (select ics.i_brand_id brand_id
     ,ics.i_class_id class_id
     ,ics.i_category_id category_id
 from {{catalog_sales}} as catalog_sales
     cross join {{item}} ics
     cross join {{date_dim}} d2
 where cs_item_sk = ics.i_item_sk
   and cs_sold_date_sk = d2.d_date_sk
   and d2.d_year between 2000 AND 2000 + 2);
$bla3 = (select iws.i_brand_id brand_id
     ,iws.i_class_id class_id
     ,iws.i_category_id category_id
 from {{web_sales}}
     cross join {{item}} iws
     cross join {{date_dim}} d3
 where ws_item_sk = iws.i_item_sk
   and ws_sold_date_sk = d3.d_date_sk
   and d3.d_year between 2000 AND 2000 + 2);

$cross_items = (select i_item_sk ss_item_sk
 from {{item}} as item cross join
 (select bla1.brand_id as brand_id, bla1.class_id as class_id, bla1.category_id as category_id from
 any
 $bla1 bla1 left semi join $bla2 bla2 on (bla1.brand_id = bla2.brand_id and bla1.class_id = bla2.class_id and bla1.category_id = bla2.category_id)
            left semi join $bla3 bla3 on (bla1.brand_id = bla3.brand_id and bla1.class_id = bla3.class_id and bla1.category_id = bla3.category_id)
 ) x
 where i_brand_id = brand_id
      and i_class_id = class_id
      and i_category_id = category_id
);

$avg_sales =
 (select avg(quantity*list_price) average_sales
  from (select ss_quantity quantity
             ,ss_list_price list_price
       from {{store_sales}} as store_sales
           cross join {{date_dim}} as date_dim
       where ss_sold_date_sk = d_date_sk
         and d_year between 2000 and 2000 + 2
       union all
       select cs_quantity quantity
             ,cs_list_price list_price
       from {{catalog_sales}} as catalog_sales
           cross join {{date_dim}} as date_dim
       where cs_sold_date_sk = d_date_sk
         and d_year between 2000 and 2000 + 2
       union all
       select ws_quantity quantity
             ,ws_list_price list_price
       from {{web_sales}} as web_sales
           cross join {{date_dim}} as date_dim
       where ws_sold_date_sk = d_date_sk
          and d_year between 2000 and 2000 + 2) x);

$week_seq_2001 = (select d_week_seq
                     from {{date_dim}} as date_dim
                     where d_year = 2000 + 1
                       and d_moy = 12
                       and d_dom = 15);

$week_seq_2000 = (select d_week_seq
                     from {{date_dim}} as date_dim
                     where d_year = 2000
                       and d_moy = 12
                       and d_dom = 15);


-- start query 1 in stream 0 using template query14.tpl and seed 1819994127

  select  channel, i_brand_id,i_class_id,i_category_id,sum(sales), sum(number_sales)
 from(
       select 'store' channel, item.i_brand_id i_brand_id,item.i_class_id i_class_id
             ,item.i_category_id i_category_id,sum(ss_quantity*ss_list_price) sales
             , count(*) number_sales
       from {{store_sales}} as store_sales
           cross join {{item}} as item
           cross join {{date_dim}} as date_dim
       where ss_item_sk in $cross_items
         and ss_item_sk = i_item_sk
         and ss_sold_date_sk = d_date_sk
         and d_year = 2000+2
         and d_moy = 11
       group by item.i_brand_id,item.i_class_id,item.i_category_id
       having sum(ss_quantity*ss_list_price) > $avg_sales
       union all
       select 'catalog' channel, item.i_brand_id i_brand_id,item.i_class_id i_class_id,item.i_category_id i_category_id, sum(cs_quantity*cs_list_price) sales, count(*) number_sales
       from {{catalog_sales}} as catalog_sales
           cross join {{item}} as item
           cross join {{date_dim}} as date_sim
       where cs_item_sk in $cross_items
         and cs_item_sk = i_item_sk
         and cs_sold_date_sk = d_date_sk
         and d_year = 2000+2
         and d_moy = 11
       group by item.i_brand_id,item.i_class_id,item.i_category_id
       having sum(cs_quantity*cs_list_price) > $avg_sales
       union all
       select 'web' channel, item.i_brand_id i_brand_id,item.i_class_id i_class_id,item.i_category_id i_category_id, sum(ws_quantity*ws_list_price) sales , count(*) number_sales
       from {{web_sales}} as web_sales
           cross join {{item}} as item
           cross join {{date_dim}} as date_dim
       where ws_item_sk in $cross_items
         and ws_item_sk = i_item_sk
         and ws_sold_date_sk = d_date_sk
         and d_year = 2000+2
         and d_moy = 11
       group by item.i_brand_id,item.i_class_id,item.i_category_id
       having sum(ws_quantity*ws_list_price) > $avg_sales
 ) y
 group by rollup (channel, i_brand_id,i_class_id,i_category_id)
 order by channel,i_brand_id,i_class_id,i_category_id
 limit 100;

select  this_year.channel ty_channel
                           ,this_year.i_brand_id ty_brand
                           ,this_year.i_class_id ty_class
                           ,this_year.i_category_id ty_category
                           ,this_year.sales ty_sales
                           ,this_year.number_sales ty_number_sales
                           ,last_year.channel ly_channel
                           ,last_year.i_brand_id ly_brand
                           ,last_year.i_class_id ly_class
                           ,last_year.i_category_id ly_category
                           ,last_year.sales ly_sales
                           ,last_year.number_sales ly_number_sales
 from
 (select 'store' channel, item.i_brand_id i_brand_id,item.i_class_id i_class_id,item.i_category_id i_category_id
        ,sum(ss_quantity*ss_list_price) sales, count(*) number_sales
 from {{store_sales}} as store_sales
     cross join {{item}} as item
     cross join {{date_dim}} as date_dim
 where ss_item_sk in $cross_items
   and ss_item_sk = i_item_sk
   and ss_sold_date_sk = d_date_sk
   and d_week_seq = $week_seq_2001
 group by item.i_brand_id,item.i_class_id,item.i_category_id
 having sum(ss_quantity*ss_list_price) > $avg_sales) this_year cross join
 (select 'store' channel, item.i_brand_id i_brand_id,item.i_class_id i_class_id
        ,item.i_category_id i_category_id, sum(ss_quantity*ss_list_price) sales, count(*) number_sales
 from {{store_sales}} as store_sales
     cross join {{item}} as item
     cross join {{date_dim}} as date_dim
 where ss_item_sk in $cross_items
   and ss_item_sk = i_item_sk
   and ss_sold_date_sk = d_date_sk
   and d_week_seq = $week_seq_2000
 group by item.i_brand_id,item.i_class_id,item.i_category_id
 having sum(ss_quantity*ss_list_price) > $avg_sales) last_year
 where this_year.i_brand_id= last_year.i_brand_id
   and this_year.i_class_id = last_year.i_class_id
   and this_year.i_category_id = last_year.i_category_id
 order by ty_channel, ty_brand, ty_class, ty_category
 limit 100;

-- end query 1 in stream 0 using template query14.tpl
