--!syntax_pg
--TPC-DS Q98

-- start query 1 in stream 0 using template ../query_templates/query98.tpl
select i_item_id
      ,i_item_desc 
      ,i_category 
      ,i_class 
      ,i_current_price
      ,sum(ss_ext_sales_price) as itemrevenue 
      ,sum(ss_ext_sales_price)*100::numeric/sum(sum(ss_ext_sales_price)) over
          (partition by i_class) as revenueratio
from	
	plato.store_sales
    	,plato.item 
    	,plato.date_dim
where 
	ss_item_sk = i_item_sk 
  	and i_category in ('Jewelry', 'Sports', 'Books')
  	and ss_sold_date_sk = d_date_sk
	and d_date between cast('2001-01-12' as date) 
				and (cast('2001-01-12' as date) + interval '30' day)::date
group by 
	i_item_id
        ,i_item_desc 
        ,i_category
        ,i_class
        ,i_current_price
order by 
	i_category
        ,i_class
        ,i_item_id
        ,i_item_desc
        ,revenueratio;

-- end query 1 in stream 0 using template ../query_templates/query98.tpl
