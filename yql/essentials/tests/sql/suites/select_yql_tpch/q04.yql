PRAGMA YqlSelect = 'force';

SELECT
    o_orderpriority,
    Count(*) AS order_count
FROM (
    VALUES
        (1, 1, Date('1993-07-01'))
) AS orders (
    o_orderkey,
    o_orderpriority,
    o_orderdate
)
WHERE
    o_orderdate >= Date('1993-07-01')
    AND o_orderdate < DateTime::MakeDate(DateTime::ShiftMonths(Date('1993-07-01'), 3))
    AND EXISTS (
        SELECT
            *
        FROM (
            VALUES
                (1, Date('1993-07-01'), Date('1993-07-01'))
        ) AS lineitem (
            l_orderkey,
            l_commitdate,
            l_receiptdate
        )
        WHERE
            l_orderkey == o_orderkey
            AND l_commitdate < l_receiptdate
    )
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority
;
