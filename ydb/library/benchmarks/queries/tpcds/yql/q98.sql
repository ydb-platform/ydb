{% include 'header.sql.jinja' %}

select i.i_item_id
      ,i.i_item_desc
      ,i.i_category
      ,i.i_class
      ,i.i_current_price
      ,sum(ss.ss_ext_sales_price) as itemrevenue
      ,sum(ss.ss_ext_sales_price)*100/sum(sum(ss.ss_ext_sales_price)) over
          (partition by i.i_class) as revenueratio
from {{store_sales}} as ss
cross join {{item}} as i
cross join {{date_dim}} as d
where
	ss.ss_item_sk = i.i_item_sk
  	and i.i_category in ('Sports', 'Books', 'Home')
  	and ss.ss_sold_date_sk = d.d_date_sk
	and cast(d.d_date as Date) between cast('1999-02-22' as Date)
				               and (cast('1999-02-22' as Date) + DateTime::IntervalFromDays(30))
group by
	 i.i_item_id
    ,i.i_item_desc
    ,i.i_category
    ,i.i_class
    ,i.i_current_price
order by
	 i.i_category
    ,i.i_class
    ,i.i_item_id
    ,i.i_item_desc
    ,revenueratio;

