-- TPC-H/TPC-R Suppliers Who Kept Orders Waiting Query (Q21)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG
$n =
    SELECT
        n_nationkey
    FROM plato.nation
    WHERE n_name == 'EGYPT';

$s =
    SELECT
        s_name,
        s_suppkey
    FROM plato.supplier
        AS supplier
    JOIN $n
        AS nation
    ON supplier.s_nationkey == nation.n_nationkey;

$l =
    SELECT
        l_suppkey,
        l_orderkey
    FROM plato.lineitem
    WHERE l_receiptdate > l_commitdate;

$j1 =
    SELECT
        s_name,
        l_suppkey,
        l_orderkey
    FROM $l
        AS l1
    JOIN $s
        AS supplier
    ON l1.l_suppkey == supplier.s_suppkey;

$j1_1 =
    SELECT
        l1.l_orderkey AS l_orderkey
    FROM $j1
        AS l1
    JOIN $l
        AS l3
    ON l1.l_orderkey == l3.l_orderkey
    WHERE l3.l_suppkey != l1.l_suppkey;

$j2 =
    SELECT
        s_name,
        l_suppkey,
        l_orderkey
    FROM $j1
        AS l1
    LEFT ONLY JOIN $j1_1
        AS l3
    ON l1.l_orderkey == l3.l_orderkey;

$j2_1 =
    SELECT
        l1.l_orderkey AS l_orderkey
    FROM $j2
        AS l1
    JOIN plato.lineitem
        AS l2
    ON l1.l_orderkey == l2.l_orderkey
    WHERE l2.l_suppkey != l1.l_suppkey;

$j3 =
    SELECT
        s_name,
        l1.l_suppkey AS l_suppkey,
        l1.l_orderkey AS l_orderkey
    FROM $j2
        AS l1
    LEFT SEMI JOIN $j2_1
        AS l2
    ON l1.l_orderkey == l2.l_orderkey;

$j4 =
    SELECT
        s_name
    FROM $j3
        AS l1
    JOIN plato.orders
        AS orders
    ON orders.o_orderkey == l1.l_orderkey
    WHERE o_orderstatus == 'F';

SELECT
    s_name,
    count(*) AS numwait
FROM $j4
GROUP BY
    s_name
ORDER BY
    numwait DESC,
    s_name
LIMIT 100;
