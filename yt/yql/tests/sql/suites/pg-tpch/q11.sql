--!syntax_pg
--TPC-H Q11
-- ignore runonopt plan diff


select
ps_partkey, 
sum(ps_supplycost * ps_availqty::numeric) as value
from 
plato."partsupp", 
plato."supplier",
plato."nation"
where 
ps_suppkey = s_suppkey
and s_nationkey = n_nationkey
and n_name = 'GERMANY'
group by 
ps_partkey having 
sum(ps_supplycost * ps_availqty::numeric) > (
select 
sum(ps_supplycost * ps_availqty::numeric) * 0.0001::numeric
from 
plato."partsupp", 
plato."supplier", 
plato."nation"
where 
ps_suppkey = s_suppkey
and s_nationkey = n_nationkey
and n_name = 'GERMANY'
)
order by
value desc;
