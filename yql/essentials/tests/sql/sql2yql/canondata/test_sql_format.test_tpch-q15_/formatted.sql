-- ignore runonopt plan diff
-- TPC-H/TPC-R Top Supplier Query (Q15)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG
$border = Date('1997-03-01');

$revenue0 = (
    SELECT
        l_suppkey AS supplier_no,
        sum(l_extendedprice * (1 - l_discount)) AS total_revenue,
        CAST(sum(l_extendedprice * (1 - l_discount)) AS Uint64) AS total_revenue_approx
    FROM
        plato.lineitem
    WHERE
        CAST(l_shipdate AS timestamp) >= $border
        AND CAST(l_shipdate AS timestamp) < ($border + Interval('P92D'))
    GROUP BY
        l_suppkey
);

$max_revenue = (
    SELECT
        max(total_revenue_approx) AS max_revenue
    FROM
        $revenue0
);

$join1 = (
    SELECT
        s.s_suppkey AS s_suppkey,
        s.s_name AS s_name,
        s.s_address AS s_address,
        s.s_phone AS s_phone,
        r.total_revenue AS total_revenue,
        r.total_revenue_approx AS total_revenue_approx
    FROM
        plato.supplier AS s
    JOIN
        $revenue0 AS r
    ON
        s.s_suppkey == r.supplier_no
);

SELECT
    j.s_suppkey AS s_suppkey,
    j.s_name AS s_name,
    j.s_address AS s_address,
    j.s_phone AS s_phone,
    j.total_revenue AS total_revenue
FROM
    $join1 AS j
JOIN
    $max_revenue AS m
ON
    j.total_revenue_approx == m.max_revenue
ORDER BY
    s_suppkey
;
