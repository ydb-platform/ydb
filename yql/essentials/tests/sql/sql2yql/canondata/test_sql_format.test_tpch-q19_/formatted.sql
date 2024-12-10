-- TPC-H/TPC-R Discounted Revenue Query (Q19)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG
SELECT
    sum(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM
    plato.lineitem AS l
JOIN
    plato.part AS p
ON
    p.p_partkey == l.l_partkey
WHERE
    (
        p.p_brand == 'Brand#23'
        AND (p.p_container == 'SM CASE' OR p.p_container == 'SM BOX' OR p.p_container == 'SM PACK' OR p.p_container == 'SM PKG')
        AND l.l_quantity >= 7 AND l.l_quantity <= 7 + 10
        AND p.p_size BETWEEN 1 AND 5
        AND (l.l_shipmode == 'AIR' OR l.l_shipmode == 'AIR REG')
        AND l.l_shipinstruct == 'DELIVER IN PERSON'
    )
    OR
    (
        p.p_brand == 'Brand#15'
        AND (p.p_container == 'MED BAG' OR p.p_container == 'MED BOX' OR p.p_container == 'MED PKG' OR p.p_container == 'MED PACK')
        AND l.l_quantity >= 17 AND l.l_quantity <= 17 + 10
        AND p.p_size BETWEEN 1 AND 10
        AND (l.l_shipmode == 'AIR' OR l.l_shipmode == 'AIR REG')
        AND l.l_shipinstruct == 'DELIVER IN PERSON'
    )
    OR
    (
        p.p_brand == 'Brand#44'
        AND (p.p_container == 'LG CASE' OR p.p_container == 'LG BOX' OR p.p_container == 'LG PACK' OR p.p_container == 'LG PKG')
        AND l.l_quantity >= 25 AND l.l_quantity <= 25 + 10
        AND p.p_size BETWEEN 1 AND 15
        AND (l.l_shipmode == 'AIR' OR l.l_shipmode == 'AIR REG')
        AND l.l_shipinstruct == 'DELIVER IN PERSON'
    )
;
