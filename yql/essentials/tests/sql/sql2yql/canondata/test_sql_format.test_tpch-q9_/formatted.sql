-- TPC-H/TPC-R Product Type Profit Measure Query (Q9)
-- Approved February 1998
-- using 1680793381 as a seed to the RNG
$p = (
    SELECT
        p_partkey,
        p_name
    FROM plato.part
    WHERE FIND(p_name, 'rose') IS NOT NULL
);

$j1 = (
    SELECT
        ps_partkey,
        ps_suppkey,
        ps_supplycost
    FROM plato.partsupp
        AS ps
    JOIN $p
        AS p
    ON ps.ps_partkey == p.p_partkey
);

$j2 = (
    SELECT
        l_suppkey,
        l_partkey,
        l_orderkey,
        l_extendedprice,
        l_discount,
        ps_supplycost,
        l_quantity
    FROM plato.lineitem
        AS l
    JOIN $j1
        AS j
    ON l.l_suppkey == j.ps_suppkey AND l.l_partkey == j.ps_partkey
);

$j3 = (
    SELECT
        l_orderkey,
        s_nationkey,
        l_extendedprice,
        l_discount,
        ps_supplycost,
        l_quantity
    FROM plato.supplier
        AS s
    JOIN $j2
        AS j
    ON j.l_suppkey == s.s_suppkey
);

$j4 = (
    SELECT
        o_orderdate,
        l_extendedprice,
        l_discount,
        ps_supplycost,
        l_quantity,
        s_nationkey
    FROM plato.orders
        AS o
    JOIN $j3
        AS j
    ON o.o_orderkey == j.l_orderkey
);

$j5 = (
    SELECT
        n_name,
        o_orderdate,
        l_extendedprice,
        l_discount,
        ps_supplycost,
        l_quantity
    FROM plato.nation
        AS n
    JOIN $j4
        AS j
    ON j.s_nationkey == n.n_nationkey
);

$profit = (
    SELECT
        n_name AS nation,
        DateTime::GetYear(CAST(o_orderdate AS timestamp)) AS o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
    FROM $j5
);

SELECT
    nation,
    o_year,
    sum(amount) AS sum_profit
FROM $profit
GROUP BY
    nation,
    o_year
ORDER BY
    nation,
    o_year DESC;
