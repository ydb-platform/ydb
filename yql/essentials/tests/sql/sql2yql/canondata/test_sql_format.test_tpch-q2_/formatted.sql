-- TPC-H/TPC-R Minimum Cost Supplier Query (Q2)
-- using 1680793381 as a seed to the RNG
$r = (
    SELECT
        r_regionkey
    FROM plato.region
    WHERE r_name == 'AMERICA'
);

$j1 = (
    SELECT
        n_name,
        n_nationkey
    FROM plato.nation
        AS n
    JOIN $r
        AS r
    ON n.n_regionkey == r.r_regionkey
);

$j2 = (
    SELECT
        s_acctbal,
        s_name,
        s_address,
        s_phone,
        s_comment,
        n_name,
        s_suppkey
    FROM plato.supplier
        AS s
    JOIN $j1
        AS j
    ON s.s_nationkey == j.n_nationkey
);

$j3 = (
    SELECT
        ps_partkey,
        ps_supplycost,
        s_acctbal,
        s_name,
        s_address,
        s_phone,
        s_comment,
        n_name
    FROM plato.partsupp
        AS ps
    JOIN $j2
        AS j
    ON ps.ps_suppkey == j.s_suppkey
);

$min_ps_supplycost = (
    SELECT
        min(ps_supplycost) AS min_ps_supplycost,
        ps_partkey
    FROM $j3
    GROUP BY
        ps_partkey
);

$p = (
    SELECT
        p_partkey,
        p_mfgr
    FROM plato.part
    WHERE p_size == 10
    AND p_type LIKE '%COPPER'
);

$j4 = (
    SELECT
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment
    FROM $p
        AS p
    JOIN $j3
        AS j
    ON p.p_partkey == j.ps_partkey
    JOIN $min_ps_supplycost
        AS m
    ON p.p_partkey == m.ps_partkey
    WHERE min_ps_supplycost == ps_supplycost
);

SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
FROM $j4
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
LIMIT 100;
