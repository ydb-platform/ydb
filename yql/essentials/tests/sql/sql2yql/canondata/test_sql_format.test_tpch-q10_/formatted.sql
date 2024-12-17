-- TPC-H/TPC-R Returned Item Reporting Query (Q10)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG
$border = Date('1993-12-01');

$join1 = (
    SELECT
        c.c_custkey AS c_custkey,
        c.c_name AS c_name,
        c.c_acctbal AS c_acctbal,
        c.c_address AS c_address,
        c.c_phone AS c_phone,
        c.c_comment AS c_comment,
        c.c_nationkey AS c_nationkey,
        o.o_orderkey AS o_orderkey
    FROM
        plato.customer AS c
    JOIN
        plato.orders AS o
    ON
        c.c_custkey == o.o_custkey
    WHERE
        CAST(o.o_orderdate AS timestamp) >= $border
        AND CAST(o.o_orderdate AS timestamp) < ($border + Interval('P90D'))
);

$join2 = (
    SELECT
        j.c_custkey AS c_custkey,
        j.c_name AS c_name,
        j.c_acctbal AS c_acctbal,
        j.c_address AS c_address,
        j.c_phone AS c_phone,
        j.c_comment AS c_comment,
        j.c_nationkey AS c_nationkey,
        l.l_extendedprice AS l_extendedprice,
        l.l_discount AS l_discount
    FROM
        $join1 AS j
    JOIN
        plato.lineitem AS l
    ON
        l.l_orderkey == j.o_orderkey
    WHERE
        l.l_returnflag == 'R'
);

$join3 = (
    SELECT
        j.c_custkey AS c_custkey,
        j.c_name AS c_name,
        j.c_acctbal AS c_acctbal,
        j.c_address AS c_address,
        j.c_phone AS c_phone,
        j.c_comment AS c_comment,
        j.c_nationkey AS c_nationkey,
        j.l_extendedprice AS l_extendedprice,
        j.l_discount AS l_discount,
        n.n_name AS n_name
    FROM
        $join2 AS j
    JOIN
        plato.nation AS n
    ON
        n.n_nationkey == j.c_nationkey
);

SELECT
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM
    $join3
GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
ORDER BY
    revenue DESC
LIMIT 20;
