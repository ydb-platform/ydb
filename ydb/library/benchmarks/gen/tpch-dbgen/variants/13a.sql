-- $ID$
-- TPC-H/TPC-R Customer Distribution Query (Q13)
-- Variant A
-- Approved March 1998
:x
create view orders_per_cust:s (custkey, ordercount) as
	select
		c_custkey,
		count(o_orderkey)
	from
		customer left outer join orders on
			c_custkey = o_custkey
			and o_comment not like '%:1%:2%'
	group by
		c_custkey;

:o
select
	ordercount,
	count(*) as custdist
from
	orders_per_cust:s
group by
	ordercount
order by
	custdist desc,
	ordercount desc;

drop view orders_per_cust:s;
:n -1
