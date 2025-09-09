--!syntax_pg
--ignore runonopt plan diff
--TPC-H Q17


select
sum(l_extendedprice) / 7.0::numeric as avg_yearly
from 
plato."lineitem", 
plato."part"
where 
p_partkey = l_partkey
and p_brand = 'Brand#23'
and p_container = 'MED BOX'
and l_quantity < (
select
0.2::numeric * avg(l_quantity)
from 
plato."lineitem"
where 
l_partkey = p_partkey
);
