{% include 'header.sql.jinja' %}

-- start query 1 in stream 0 using template query53.tpl and seed 1819994127
select  * from
(select item.i_manufact_id as i_manufact_id,
sum(ss_sales_price) sum_sales,
avg(sum(ss_sales_price)) over (partition by item.i_manufact_id) avg_quarterly_sales
from {{item}} as item
cross join {{store_sales}} as store_sales
cross join {{date_dim}} as date_dim
cross join {{store}} as store
where ss_item_sk = i_item_sk and
ss_sold_date_sk = d_date_sk and
ss_store_sk = s_store_sk and
d_month_seq in (1212,1212+1,1212+2,1212+3,1212+4,1212+5,1212+6,1212+7,1212+8,1212+9,1212+10,1212+11) and
((i_category in ('Books','Children','Electronics') and
i_class in ('personal','portable','reference','self-help') and
i_brand in ('scholaramalgamalg #14','scholaramalgamalg #7',
		'exportiunivamalg #9','scholaramalgamalg #9'))
or(i_category in ('Women','Music','Men') and
i_class in ('accessories','classical','fragrances','pants') and
i_brand in ('amalgimporto #1','edu packscholar #1','exportiimporto #1',
		'importoamalg #1')))
group by item.i_manufact_id, date_dim.d_qoy ) tmp1
where case when avg_quarterly_sales > 0
	then abs (sum_sales - avg_quarterly_sales)/ avg_quarterly_sales
	else null end > $z0_1_35
order by avg_quarterly_sales,
	 sum_sales,
	 i_manufact_id
limit 100;

-- end query 1 in stream 0 using template query53.tpl
