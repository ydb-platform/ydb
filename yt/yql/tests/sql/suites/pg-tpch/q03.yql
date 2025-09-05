--!syntax_pg
--TPC-H Q3


select
l_orderkey, 
sum(l_extendedprice*(1::numeric-l_discount)) as revenue,
o_orderdate, 
o_shippriority
from 
plato."customer", 
plato."orders", 
plato."lineitem"
where 
c_mktsegment = 'BUILDING'
and c_custkey = o_custkey
and l_orderkey = o_orderkey
and o_orderdate < date '1995-03-15'
and l_shipdate > date '1995-03-15'
group by 
l_orderkey, 
o_orderdate, 
o_shippriority
order by 
revenue desc, 
o_orderdate
limit 10;
