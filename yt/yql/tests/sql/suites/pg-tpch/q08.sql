--!syntax_pg
--TPC-H Q8


select
o_year, 
sum(case 
when nation = 'BRAZIL' 
then volume
else 0::numeric
end) / (sum(volume) + 1e-12::numeric) as mkt_share
from (
select 
extract(year from o_orderdate) as o_year,
l_extendedprice * (1::numeric-l_discount) as volume, 
n2.n_name as nation
from 
plato."part", 
plato."supplier", 
plato."lineitem", 
plato."orders", 
plato."customer", 
plato."nation" n1, 
plato."nation" n2, 
plato."region"
where 
p_partkey = l_partkey
and s_suppkey = l_suppkey
and l_orderkey = o_orderkey
and o_custkey = c_custkey
and c_nationkey = n1.n_nationkey
and n1.n_regionkey = r_regionkey
and r_name = 'AMERICA'
and s_nationkey = n2.n_nationkey
and o_orderdate between date '1995-01-01' and date '1996-12-31'
and p_type = 'ECONOMY ANODIZED STEEL' 
) as all_nations
group by 
o_year
order by 
o_year;
