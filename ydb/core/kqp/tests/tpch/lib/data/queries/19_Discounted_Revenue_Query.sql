-- $ID$
-- TPC-H/TPC-R Discounted Revenue Query (Q19)
-- Functional Query Definition
-- Approved February 1998

$PRAGMAS$

select
    sum(l.l_extendedprice* (1 - l.l_discount)) as revenue
from
    `$DBROOT$/lineitem` as l join `$DBROOT$/part` as p on p.p_partkey = l.l_partkey
where
    (
        p.p_brand = 'Brand#32'
        and (p.p_container = 'SM BAG' or p.p_container = 'SM CASE' or p.p_container = 'SM BOX' or p.p_container = 'SM PACK' or p.p_container = 'SM PKG')
        and l.l_quantity >= 3 and l.l_quantity <= 3 + 10
        and p.p_size between 1 and 30
        and (l.l_shipmode = 'TRUCK' or l.l_shipmode = 'REG AIR')
        and l.l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p.p_brand = 'Brand#23'
        and (p.p_container = 'MED BAG' or p.p_container = 'MED BOX' or p.p_container = 'MED PKG' or p.p_container = 'MED PACK')
        and l.l_quantity >= 10 and l.l_quantity <= 10 + 30
        and p.p_size between 1 and 10
        and (l.l_shipmode = 'MAIL' or l.l_shipmode = 'REG AIR')
        and l.l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p.p_brand = 'Brand#44'
        and (p.p_container = 'LG CASE' or p.p_container = 'LG BOX' or p.p_container = 'LG PACK' or p.p_container = 'LG PKG')
        and l.l_quantity >= 25 and l.l_quantity <= 29 + 10
        and p.p_size between 15 and 40
        and (l.l_shipmode = 'SHIP' or l.l_shipmode = 'REG AIR')
        and l.l_shipinstruct = 'DELIVER IN PERSON'
    );
