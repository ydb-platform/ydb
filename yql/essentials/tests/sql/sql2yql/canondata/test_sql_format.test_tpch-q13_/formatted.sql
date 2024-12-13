-- TPC-H/TPC-R Customer Distribution Query (Q13)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG
$orders = (
    SELECT
        o_orderkey,
        o_custkey
    FROM
        plato.orders
    WHERE
        o_comment NOT LIKE "%unusual%requests%"
);

SELECT
    c_count AS c_count,
    count(*) AS custdist
FROM (
    SELECT
        c.c_custkey AS c_custkey,
        count(o.o_orderkey) AS c_count
    FROM
        plato.customer AS c
    LEFT OUTER JOIN
        $orders AS o
    ON
        c.c_custkey == o.o_custkey
    GROUP BY
        c.c_custkey
) AS c_orders
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC
;
