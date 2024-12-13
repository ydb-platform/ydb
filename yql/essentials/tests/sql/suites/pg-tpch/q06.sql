--!syntax_pg
--TPC-H Q6


select
sum(l_extendedprice*l_discount) as revenue
from 
plato."lineitem"
where 
l_shipdate >= date '1994-01-01'
and l_shipdate < date '1994-01-01' + interval '1' year
and l_discount between (0.06 - 0.01)::numeric and (0.06 + 0.01)::numeric
and l_quantity < 24::numeric;
