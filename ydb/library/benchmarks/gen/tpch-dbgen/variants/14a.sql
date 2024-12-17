-- $ID$
-- TPC-H/TPC-R Promotion Effect Query (Q14)
-- Variant A
-- Approved March 1998
:x
:o
select
	100.00 * sum(decode(substring(p_type from 1 for 5), 'PROMO',
		l_extendedprice * (1-l_discount), 0)) /
		sum(l_extendedprice * (1-l_discount)) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= date ':1'
	and l_shipdate < date ':1' + interval '1' month;
:n -1
