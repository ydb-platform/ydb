-- TPC-H/TPC-R Potential Part Promotion Query (Q20)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG
$border = Date("1993-01-01");

$threshold = (
    SELECT
        0.5 * sum(l_quantity) AS threshold,
        l_partkey AS l_partkey,
        l_suppkey AS l_suppkey
    FROM
        plato.lineitem
    WHERE
        CAST(l_shipdate AS timestamp) >= $border
        AND CAST(l_shipdate AS timestamp) < ($border + Interval("P365D"))
    GROUP BY
        l_partkey,
        l_suppkey
);

$parts = (
    SELECT
        p_partkey
    FROM
        plato.part
    WHERE
        StartsWith(p_name, 'maroon')
);

$join1 = (
    SELECT
        ps.ps_suppkey AS ps_suppkey,
        ps.ps_availqty AS ps_availqty,
        ps.ps_partkey AS ps_partkey
    FROM
        plato.partsupp AS ps
    JOIN ANY
        $parts AS p
    ON
        ps.ps_partkey == p.p_partkey
);

$join2 = (
    SELECT DISTINCT
        (j.ps_suppkey) AS ps_suppkey
    FROM
        $join1 AS j
    JOIN ANY
        $threshold AS t
    ON
        j.ps_partkey == t.l_partkey AND j.ps_suppkey == t.l_suppkey
    WHERE
        j.ps_availqty > t.threshold
);

$join3 = (
    SELECT
        j.ps_suppkey AS ps_suppkey,
        s.s_name AS s_name,
        s.s_address AS s_address,
        s.s_nationkey AS s_nationkey
    FROM
        $join2 AS j
    JOIN ANY
        plato.supplier AS s
    ON
        j.ps_suppkey == s.s_suppkey
);

SELECT
    j.s_name AS s_name,
    j.s_address AS s_address
FROM
    $join3 AS j
JOIN
    plato.nation AS n
ON
    j.s_nationkey == n.n_nationkey
WHERE
    n.n_name == 'VIETNAM'
ORDER BY
    s_name
;
