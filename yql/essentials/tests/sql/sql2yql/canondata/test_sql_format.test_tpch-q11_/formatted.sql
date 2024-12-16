-- TPC-H/TPC-R Important Stock Identification Query (Q11)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG
PRAGMA DisableSimpleColumns;

$join1 = (
    SELECT
        ps.ps_partkey AS ps_partkey,
        ps.ps_supplycost AS ps_supplycost,
        ps.ps_availqty AS ps_availqty,
        s.s_nationkey AS s_nationkey
    FROM
        plato.partsupp AS ps
    JOIN
        plato.supplier AS s
    ON
        ps.ps_suppkey == s.s_suppkey
);

$join2 = (
    SELECT
        j.ps_partkey AS ps_partkey,
        j.ps_supplycost AS ps_supplycost,
        j.ps_availqty AS ps_availqty,
        j.s_nationkey AS s_nationkey
    FROM
        $join1 AS j
    JOIN
        plato.nation AS n
    ON
        n.n_nationkey == j.s_nationkey
    WHERE
        n.n_name == 'CANADA'
);

$threshold = (
    SELECT
        sum(ps_supplycost * ps_availqty) * 0.0001000000 AS threshold
    FROM
        $join2
);

$values = (
    SELECT
        ps_partkey,
        sum(ps_supplycost * ps_availqty) AS value
    FROM
        $join2
    GROUP BY
        ps_partkey
);

SELECT
    v.ps_partkey AS ps_partkey,
    v.value AS value
FROM
    $values AS v
CROSS JOIN
    $threshold AS t
WHERE
    v.value > t.threshold
ORDER BY
    value DESC
;
