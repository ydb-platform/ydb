PRAGMA YqlSelect = 'force';
PRAGMA AnsiImplicitCrossJoin;

SELECT
    l_shipmode,
    Sum(
        CASE
            WHEN o_orderpriority == '1-URGENT'
            OR o_orderpriority == '2-HIGH' THEN 1
            ELSE 0
        END
    ) AS high_line_count,
    Sum(
        CASE
            WHEN o_orderpriority != '1-URGENT'
            AND o_orderpriority != '2-HIGH' THEN 1
            ELSE 0
        END
    ) AS low_line_count
FROM (
    VALUES
        (1, '1-URGENT')
) AS orders (
    o_orderkey,
    o_orderpriority
)
, (
    VALUES
        (1, Date('1994-01-01'), Date('1994-01-01'), Date('1994-01-01'), 'MAIL')
) AS lineitem (
    l_orderkey,
    l_shipdate,
    l_commitdate,
    l_receiptdate,
    l_shipmode
)
WHERE
    o_orderkey == l_orderkey
    AND l_shipmode IN ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= Date('1994-01-01')
    AND l_receiptdate < DateTime::MakeDate(DateTime::ShiftYears(Date('1994-01-01'), 1))
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode
;
