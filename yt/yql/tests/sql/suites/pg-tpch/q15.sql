--!syntax_pg
--ignore runonopt plan diff
--TPC-H Q15


create view revenue_STREAM_ID (supplier_no, total_revenue) as
select 
l_suppkey, 
sum(l_extendedprice * (1::numeric - l_discount))
from 
plato."lineitem"
where 
l_shipdate >= date '1996-01-01'
and l_shipdate < date '1996-01-01' + interval '3' month
group by 
l_suppkey;
select
s_suppkey, 
s_name, 
s_address, 
s_phone, 
total_revenue
from 
plato."supplier", 
revenue_STREAM_ID
where 
s_suppkey = supplier_no
and total_revenue = (
select 
max(total_revenue)
from 
revenue_STREAM_ID
)
order by 
s_suppkey;
drop view revenue_STREAM_ID;
