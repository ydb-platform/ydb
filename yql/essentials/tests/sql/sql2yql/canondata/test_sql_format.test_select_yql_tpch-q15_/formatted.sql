PRAGMA YqlSelect = 'force';
PRAGMA AnsiImplicitCrossJoin;

$revenue = (
    SELECT
        l_suppkey AS supplier_no,
        Sum(l_extendedprice * (1 - l_discount)) AS total_revenue
    FROM (
        VALUES
            (1, 1, 1.0, 1.0, Date('1996-01-01'))
    ) AS lineitem (
        l_orderkey,
        l_suppkey,
        l_extendedprice,
        l_discount,
        l_shipdate
    )
    WHERE
        l_shipdate >= Date('1996-01-01')
        AND l_shipdate < DateTime::MakeDate(DateTime::ShiftMonths(Date('1996-01-01'), 3))
    GROUP BY
        l_suppkey
);

SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM (
    VALUES
        (1, 'x', 'y', 'z')
) AS supplier (
    s_suppkey,
    s_name,
    s_address,
    s_phone
)
,
    $revenue
WHERE
    s_suppkey == supplier_no
    AND total_revenue == (
        SELECT
            Max(total_revenue)
        FROM
            $revenue
    )
ORDER BY
    s_suppkey
;
