--!syntax_pg
--TPC-H Q20


select 
s_name, 
s_address
from 
plato."supplier",
plato."nation"
where 
s_suppkey in (
select 
ps_suppkey
from 
plato."partsupp"
where 
ps_partkey in (
select 
p_partkey
from 
plato."part"
where 
p_name like 'forest%'
)
and ps_availqty::numeric > (
select 
0.5::numeric * sum(l_quantity)
from 
plato."lineitem"
where 
l_partkey = ps_partkey
and l_suppkey = ps_suppkey
and l_shipdate >= '1994-01-01'::date
and l_shipdate < '1994-01-01'::date + interval '1' year 
)
)
and s_nationkey = n_nationkey
and n_name = 'CANADA'
order by 
s_name;
