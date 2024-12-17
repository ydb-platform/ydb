-- TPC-H/TPC-R Local Supplier Volume Query (Q5)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG
$join1 = (
    SELECT
        o.o_orderkey AS o_orderkey,
        o.o_orderdate AS o_orderdate,
        c.c_nationkey AS c_nationkey
    FROM
        plato.customer AS c
    JOIN
        plato.orders AS o
    ON
        c.c_custkey == o.o_custkey
);

$join2 = (
    SELECT
        j.o_orderkey AS o_orderkey,
        j.o_orderdate AS o_orderdate,
        j.c_nationkey AS c_nationkey,
        l.l_extendedprice AS l_extendedprice,
        l.l_discount AS l_discount,
        l.l_suppkey AS l_suppkey
    FROM
        $join1 AS j
    JOIN
        plato.lineitem AS l
    ON
        l.l_orderkey == j.o_orderkey
);

$join3 = (
    SELECT
        j.o_orderkey AS o_orderkey,
        j.o_orderdate AS o_orderdate,
        j.c_nationkey AS c_nationkey,
        j.l_extendedprice AS l_extendedprice,
        j.l_discount AS l_discount,
        j.l_suppkey AS l_suppkey,
        s.s_nationkey AS s_nationkey
    FROM
        $join2 AS j
    JOIN
        plato.supplier AS s
    ON
        j.l_suppkey == s.s_suppkey
);

$join4 = (
    SELECT
        j.o_orderkey AS o_orderkey,
        j.o_orderdate AS o_orderdate,
        j.c_nationkey AS c_nationkey,
        j.l_extendedprice AS l_extendedprice,
        j.l_discount AS l_discount,
        j.l_suppkey AS l_suppkey,
        j.s_nationkey AS s_nationkey,
        n.n_regionkey AS n_regionkey,
        n.n_name AS n_name
    FROM
        $join3 AS j
    JOIN
        plato.nation AS n
    ON
        j.s_nationkey == n.n_nationkey
        AND j.c_nationkey == n.n_nationkey
);

$join5 = (
    SELECT
        j.o_orderkey AS o_orderkey,
        j.o_orderdate AS o_orderdate,
        j.c_nationkey AS c_nationkey,
        j.l_extendedprice AS l_extendedprice,
        j.l_discount AS l_discount,
        j.l_suppkey AS l_suppkey,
        j.s_nationkey AS s_nationkey,
        j.n_regionkey AS n_regionkey,
        j.n_name AS n_name,
        r.r_name AS r_name
    FROM
        $join4 AS j
    JOIN
        plato.region AS r
    ON
        j.n_regionkey == r.r_regionkey
);

$border = Date('1995-01-01');

SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    $join5
WHERE
    r_name == 'AFRICA'
    AND CAST(o_orderdate AS Timestamp) >= $border
    AND CAST(o_orderdate AS Timestamp) < ($border + Interval('P365D'))
GROUP BY
    n_name
ORDER BY
    revenue DESC
;
