-- TPC-H/TPC-R Shipping Priority Query (Q3)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG
$join1 = (
    SELECT
        c.c_mktsegment AS c_mktsegment,
        o.o_orderdate AS o_orderdate,
        o.o_shippriority AS o_shippriority,
        o.o_orderkey AS o_orderkey
    FROM
        plato.customer AS c
    JOIN
        plato.orders AS o
    ON
        c.c_custkey == o.o_custkey
);

$join2 = (
    SELECT
        j1.c_mktsegment AS c_mktsegment,
        j1.o_orderdate AS o_orderdate,
        j1.o_shippriority AS o_shippriority,
        l.l_orderkey AS l_orderkey,
        l.l_discount AS l_discount,
        l.l_shipdate AS l_shipdate,
        l.l_extendedprice AS l_extendedprice
    FROM
        $join1 AS j1
    JOIN
        plato.lineitem AS l
    ON
        l.l_orderkey == j1.o_orderkey
);

SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    $join2
WHERE
    c_mktsegment == 'MACHINERY'
    AND CAST(o_orderdate AS Timestamp) < Date('1995-03-08')
    AND CAST(l_shipdate AS Timestamp) > Date('1995-03-08')
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10;
