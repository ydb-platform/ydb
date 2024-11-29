-- TPC-H/TPC-R National Market Share Query (Q8)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG
$join1 = (
    SELECT
        l.l_extendedprice * (1 - l.l_discount) AS volume,
        l.l_suppkey AS l_suppkey,
        l.l_orderkey AS l_orderkey
    FROM plato.part
        AS p
    JOIN plato.lineitem
        AS l
    ON p.p_partkey == l.l_partkey
    WHERE p.p_type == 'ECONOMY PLATED COPPER'
);

$join2 = (
    SELECT
        j.volume AS volume,
        j.l_orderkey AS l_orderkey,
        s.s_nationkey AS s_nationkey
    FROM $join1
        AS j
    JOIN plato.supplier
        AS s
    ON s.s_suppkey == j.l_suppkey
);

$join3 = (
    SELECT
        j.volume AS volume,
        j.l_orderkey AS l_orderkey,
        n.n_name AS nation
    FROM $join2
        AS j
    JOIN plato.nation
        AS n
    ON n.n_nationkey == j.s_nationkey
);

$join4 = (
    SELECT
        j.volume AS volume,
        j.nation AS nation,
        DateTime::GetYear(CAST(o.o_orderdate AS Timestamp)) AS o_year,
        o.o_custkey AS o_custkey
    FROM $join3
        AS j
    JOIN plato.orders
        AS o
    ON o.o_orderkey == j.l_orderkey
    WHERE CAST(CAST(o_orderdate AS Timestamp) AS Date) BETWEEN Date('1995-01-01') AND Date('1996-12-31')
);

$join5 = (
    SELECT
        j.volume AS volume,
        j.nation AS nation,
        j.o_year AS o_year,
        c.c_nationkey AS c_nationkey
    FROM $join4
        AS j
    JOIN plato.customer
        AS c
    ON c.c_custkey == j.o_custkey
);

$join6 = (
    SELECT
        j.volume AS volume,
        j.nation AS nation,
        j.o_year AS o_year,
        n.n_regionkey AS n_regionkey
    FROM $join5
        AS j
    JOIN plato.nation
        AS n
    ON n.n_nationkey == j.c_nationkey
);

$join7 = (
    SELECT
        j.volume AS volume,
        j.nation AS nation,
        j.o_year AS o_year
    FROM $join6
        AS j
    JOIN plato.region
        AS r
    ON r.r_regionkey == j.n_regionkey
    WHERE r.r_name == 'AFRICA'
);

SELECT
    o_year,
    sum(
        CASE
            WHEN nation == 'MOZAMBIQUE'
                THEN volume
            ELSE 0
        END
    ) / sum(volume) AS mkt_share
FROM $join7
    AS all_nations
GROUP BY
    o_year
ORDER BY
    o_year;
