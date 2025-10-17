--!syntax_pg
--ignore runonopt plan diff
--TPC-H Q2


select
s_acctbal, 
s_name, 
n_name, 
p_partkey, 
p_mfgr, 
s_address, 
s_phone, 
s_comment
from 
plato."part", 
plato."supplier", 
plato."partsupp", 
plato."nation", 
plato."region"
where 
p_partkey = ps_partkey
and s_suppkey = ps_suppkey
and p_size = 15
and p_type like '%BRASS'
and s_nationkey = n_nationkey
and n_regionkey = r_regionkey
and r_name = 'EUROPE'
and ps_supplycost = (
select 
min(ps_supplycost)
from 
plato."partsupp",
plato."supplier", 
plato."nation",
plato."region"
where 
p_partkey = ps_partkey
and s_suppkey = ps_suppkey
and s_nationkey = n_nationkey
and n_regionkey = r_regionkey
and r_name = 'EUROPE'
)
order by 
s_acctbal desc, 
n_name, 
s_name, 
p_partkey
limit 100;
