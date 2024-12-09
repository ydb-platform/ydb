-- TPC-H/TPC-R Volume Shipping Query (Q7)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG
$n =
    SELECT
        n_name,
        n_nationkey
    FROM plato.nation
        AS n
    WHERE n_name == 'PERU' OR n_name == 'MOZAMBIQUE';

$l =
    SELECT
        l_orderkey,
        l_suppkey,
        DateTime::GetYear(CAST(l_shipdate AS timestamp)) AS l_year,
        l_extendedprice * (1 - l_discount) AS volume
    FROM plato.lineitem
        AS l
    WHERE CAST(CAST(l.l_shipdate AS Timestamp) AS Date) BETWEEN Date('1995-01-01') AND Date('1996-12-31');

$j1 =
    SELECT
        n_name AS supp_nation,
        s_suppkey
    FROM plato.supplier
        AS supplier
    JOIN $n
        AS n1
    ON supplier.s_nationkey == n1.n_nationkey;

$j2 =
    SELECT
        n_name AS cust_nation,
        c_custkey
    FROM plato.customer
        AS customer
    JOIN $n
        AS n2
    ON customer.c_nationkey == n2.n_nationkey;

$j3 =
    SELECT
        cust_nation,
        o_orderkey
    FROM plato.orders
        AS orders
    JOIN $j2
        AS customer
    ON orders.o_custkey == customer.c_custkey;

$j4 =
    SELECT
        cust_nation,
        l_orderkey,
        l_suppkey,
        l_year,
        volume
    FROM $l
        AS lineitem
    JOIN $j3
        AS orders
    ON lineitem.l_orderkey == orders.o_orderkey;

$j5 =
    SELECT
        supp_nation,
        cust_nation,
        l_year,
        volume
    FROM $j4
        AS lineitem
    JOIN $j1
        AS supplier
    ON lineitem.l_suppkey == supplier.s_suppkey
    WHERE (supp_nation == 'PERU' AND cust_nation == 'MOZAMBIQUE')
    OR (supp_nation == 'MOZAMBIQUE' AND cust_nation == 'PERU');

SELECT
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) AS revenue
FROM $j5
    AS shipping
GROUP BY
    supp_nation,
    cust_nation,
    l_year
ORDER BY
    supp_nation,
    cust_nation,
    l_year;
