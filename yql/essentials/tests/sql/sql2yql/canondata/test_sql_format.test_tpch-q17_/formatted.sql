-- TPC-H/TPC-R Small-Quantity-Order Revenue Query (Q17)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG
$threshold = (
    SELECT
        0.2 * avg(l_quantity) AS threshold,
        l_partkey
    FROM plato.lineitem
    GROUP BY
        l_partkey
);

$join1 = (
    SELECT
        p.p_partkey AS p_partkey,
        l.l_extendedprice AS l_extendedprice,
        l.l_quantity AS l_quantity
    FROM plato.lineitem
        AS l
    JOIN plato.part
        AS p
    ON p.p_partkey == l.l_partkey
    WHERE p.p_brand == 'Brand#35'
    AND p.p_container == 'LG DRUM'
);

SELECT
    sum(j.l_extendedprice) / 7.0 AS avg_yearly
FROM $join1
    AS j
JOIN $threshold
    AS t
ON j.p_partkey == t.l_partkey
WHERE j.l_quantity < t.threshold;
