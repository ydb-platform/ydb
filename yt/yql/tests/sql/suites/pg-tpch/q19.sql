--!syntax_pg
--TPC-H Q19


select
sum(l_extendedprice * (1::numeric - l_discount) ) as revenue
from 
plato."lineitem", 
plato."part"
where 
(
p_partkey = l_partkey
and p_brand = 'Brand#12'
and p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') 
and l_quantity >= 1::numeric and l_quantity <= (1 + 10)::numeric
and p_size between 1 and 5 
and l_shipmode in ('AIR', 'AIR REG')
and l_shipinstruct = 'DELIVER IN PERSON' 
)
or 
(
p_partkey = l_partkey
and p_brand = 'Brand#23'
and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
and l_quantity >= 10::numeric and l_quantity <= (10 + 10)::numeric
and p_size between 1 and 10
and l_shipmode in ('AIR', 'AIR REG')
and l_shipinstruct = 'DELIVER IN PERSON'
)
or 
(
p_partkey = l_partkey
and p_brand = 'Brand#34'
and p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
and l_quantity >= 20::numeric and l_quantity <= (20 + 10)::numeric
and p_size between 1 and 15
and l_shipmode in ('AIR', 'AIR REG')
and l_shipinstruct = 'DELIVER IN PERSON'
);
