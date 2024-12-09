--!syntax_pg
--ignore runonopt plan diff
--TPC-H Q18


select 
c_name,
c_custkey, 
o_orderkey,
o_orderdate,
o_totalprice,
sum(l_quantity)
from 
plato."customer",
plato."orders",
plato."lineitem"
where 
o_orderkey in (
select
l_orderkey
from
plato."lineitem"
group by 
l_orderkey having 
sum(l_quantity) > 300::numeric
)
and c_custkey = o_custkey
and o_orderkey = l_orderkey
group by 
c_name, 
c_custkey, 
o_orderkey, 
o_orderdate, 
o_totalprice
order by 
o_totalprice desc,
o_orderdate
limit 100;
