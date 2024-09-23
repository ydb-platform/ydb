{% include 'header.sql.jinja' %}

-- NB: Subquerys
-- start query 1 in stream 0 using template query36.tpl and seed 1544728811
select
    $upscale(sum(ss_net_profit))/$upscale(sum(ss_ext_sales_price)) as gross_margin
   ,item.i_category
   ,item.i_class
   ,grouping(item.i_category)+grouping(item.i_class) as lochierarchy
   ,rank() over (
 	partition by grouping(item.i_category)+grouping(item.i_class),
 	case when grouping(item.i_class) = 0 then item.i_category else null end
 	order by $upscale(sum(ss_net_profit))/$upscale(sum(ss_ext_sales_price)) asc) as rank_within_parent
 from
    {{store_sales}} as store_sales
   cross join {{date_dim}}       d1
   cross join {{item}} as item
   cross join {{store}} as store
 where
    d1.d_year = 1999
 and d1.d_date_sk = ss_sold_date_sk
 and item.i_item_sk  = ss_item_sk
 and s_store_sk  = ss_store_sk
 and s_state in ('IN','AL','MI','MN',
                 'TN','LA','FL','NM')
 group by rollup(item.i_category,item.i_class)
 order by
   lochierarchy desc
  ,case when lochierarchy = 0 then item.i_category else null end
  ,rank_within_parent
  limit 100;

-- end query 1 in stream 0 using template query36.tpl
