-- TPC-H/TPC-R Large Volume Customer Query (Q18)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG
$in = (
    SELECT
        l_orderkey,
        sum(l_quantity) AS sum_l_quantity
    FROM
        plato.lineitem
    GROUP BY
        l_orderkey
    HAVING
        sum(l_quantity) > 315
);

$join1 = (
    SELECT
        c.c_name AS c_name,
        c.c_custkey AS c_custkey,
        o.o_orderkey AS o_orderkey,
        o.o_orderdate AS o_orderdate,
        o.o_totalprice AS o_totalprice
    FROM
        plato.customer AS c
    JOIN
        plato.orders AS o
    ON
        c.c_custkey == o.o_custkey
);

SELECT
    j.c_name AS c_name,
    j.c_custkey AS c_custkey,
    j.o_orderkey AS o_orderkey,
    j.o_orderdate AS o_orderdate,
    j.o_totalprice AS o_totalprice,
    sum(i.sum_l_quantity) AS sum_l_quantity
FROM
    $join1 AS j
JOIN
    $in AS i
ON
    i.l_orderkey == j.o_orderkey
GROUP BY
    j.c_name,
    j.c_custkey,
    j.o_orderkey,
    j.o_orderdate,
    j.o_totalprice
ORDER BY
    o_totalprice DESC,
    o_orderdate
LIMIT 100;
