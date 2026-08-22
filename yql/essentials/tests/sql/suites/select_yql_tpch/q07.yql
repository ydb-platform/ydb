PRAGMA YqlSelect = 'force';
PRAGMA AnsiImplicitCrossJoin;

SELECT
    supp_nation,
    cust_nation,
    l_year,
    Sum(volume) AS revenue
FROM (
    SELECT
        n1.n_name AS supp_nation,
        n2.n_name AS cust_nation,
        DateTime::GetYear(CAST(l_shipdate AS Timestamp)) AS l_year,
        l_extendedprice * (1 - l_discount) AS volume
    FROM (
        VALUES
            (1, 1)
    ) AS supplier (
        s_suppkey,
        s_nationkey
    )
    , (
        VALUES
            (1, 1, Date('1995-01-01'), 1.0, 1.0)
    ) AS lineitem (
        l_suppkey,
        l_orderkey,
        l_shipdate,
        l_extendedprice,
        l_discount
    )
    , (
        VALUES
            (1, 1)
    ) AS orders (
        o_orderkey,
        o_custkey
    )
    , (
        VALUES
            (1, 1)
    ) AS customer (
        c_custkey,
        c_nationkey
    )
    , (
        VALUES
            (1, 'GERMANY')
    ) AS n1 (
        n_nationkey,
        n_name
    )
    , (
        VALUES
            (1, 'FRANCE')
    ) AS n2 (
        n_nationkey,
        n_name
    )
    WHERE
        s_suppkey == l_suppkey
        AND o_orderkey == l_orderkey
        AND c_custkey == o_custkey
        AND s_nationkey == n1.n_nationkey
        AND c_nationkey == n2.n_nationkey
        AND (
            (n1.n_name == 'FRANCE' AND n2.n_name == 'GERMANY')
            OR (n1.n_name == 'GERMANY' AND n2.n_name == 'FRANCE')
        )
        AND l_shipdate BETWEEN Date('1995-01-01') AND Date('1996-12-31')
) AS shipping
GROUP BY
    supp_nation,
    cust_nation,
    l_year
ORDER BY
    supp_nation,
    cust_nation,
    l_year
;
