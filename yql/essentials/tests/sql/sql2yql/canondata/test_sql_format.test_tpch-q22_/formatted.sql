-- TPC-H/TPC-R Global Sales Opportunity Query (Q22)
-- TPC TPC-H Parameter Substitution (Version 2.17.2 build 0)
-- using 1680793381 as a seed to the RNG
$customers = (
    SELECT
        c_acctbal,
        c_custkey,
        Substring(c_phone, 0u, 2u) AS cntrycode
    FROM
        plato.customer
    WHERE
        (Substring(c_phone, 0u, 2u) == '31' OR Substring(c_phone, 0u, 2u) == '29' OR Substring(c_phone, 0u, 2u) == '30' OR Substring(c_phone, 0u, 2u) == '26' OR Substring(c_phone, 0u, 2u) == '28' OR Substring(c_phone, 0u, 2u) == '25' OR Substring(c_phone, 0u, 2u) == '15')
);

$avg = (
    SELECT
        avg(c_acctbal) AS a
    FROM
        $customers
    WHERE
        c_acctbal > 0.00
);

$join1 = (
    SELECT
        c.c_acctbal AS c_acctbal,
        c.c_custkey AS c_custkey,
        c.cntrycode AS cntrycode
    FROM
        $customers AS c
    CROSS JOIN
        $avg AS a
    WHERE
        c.c_acctbal > a.a
);

$join2 = (
    SELECT
        j.cntrycode AS cntrycode,
        c_custkey,
        j.c_acctbal AS c_acctbal
    FROM
        $join1 AS j
    LEFT ONLY JOIN
        plato.orders AS o
    ON
        o.o_custkey == j.c_custkey
);

SELECT
    cntrycode,
    count(*) AS numcust,
    sum(c_acctbal) AS totacctbal
FROM
    $join2 AS custsale
GROUP BY
    cntrycode
ORDER BY
    cntrycode
;
