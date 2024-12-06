-- TPC-H/TPC-R Promotion Effect Query (Q14)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG
$border = Date("1994-08-01");

SELECT
    100.00 * sum(
        CASE
            WHEN StartsWith(p.p_type, 'PROMO') THEN l.l_extendedprice * (1 - l.l_discount)
            ELSE 0
        END
    ) / sum(l.l_extendedprice * (1 - l.l_discount)) AS promo_revenue
FROM
    plato.lineitem AS l
JOIN
    plato.part AS p
ON
    l.l_partkey == p.p_partkey
WHERE
    CAST(l.l_shipdate AS timestamp) >= $border
    AND CAST(l.l_shipdate AS timestamp) < ($border + Interval("P31D"))
;
