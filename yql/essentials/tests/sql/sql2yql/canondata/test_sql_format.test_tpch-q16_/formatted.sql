-- TPC-H/TPC-R Parts/Supplier Relationship Query (Q16)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG
$join = (
    SELECT
        ps.ps_suppkey AS ps_suppkey,
        ps.ps_partkey AS ps_partkey
    FROM
        plato.partsupp AS ps
    LEFT JOIN
        plato.supplier AS w
    ON
        w.s_suppkey == ps.ps_suppkey
    WHERE
        NOT (s_comment LIKE "%Customer%Complaints%")
);

SELECT
    p.p_brand AS p_brand,
    p.p_type AS p_type,
    p.p_size AS p_size,
    count(DISTINCT j.ps_suppkey) AS supplier_cnt
FROM
    $join AS j
JOIN
    plato.part AS p
ON
    p.p_partkey == j.ps_partkey
WHERE
    p.p_brand != 'Brand#33'
    AND (NOT StartsWith(p.p_type, 'PROMO POLISHED'))
    AND (p.p_size == 20 OR p.p_size == 27 OR p.p_size == 11 OR p.p_size == 45 OR p.p_size == 40 OR p.p_size == 41 OR p.p_size == 34 OR p.p_size == 36)
GROUP BY
    p.p_brand,
    p.p_type,
    p.p_size
ORDER BY
    supplier_cnt DESC,
    p_brand,
    p_type,
    p_size
;
