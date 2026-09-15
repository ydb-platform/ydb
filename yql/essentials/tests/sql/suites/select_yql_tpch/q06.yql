PRAGMA YqlSelect = 'force';

SELECT
    Sum(l_extendedprice * l_discount) AS revenue
FROM (
    VALUES
        (Date('1994-01-01'), 1.0, 0.06, 1.0)
) AS lineitem (
    l_shipdate,
    l_discount,
    l_quantity,
    l_extendedprice
)
WHERE
    l_shipdate >= Date('1994-01-01')
    AND l_shipdate < DateTime::MakeDate(DateTime::ShiftYears(Date('1994-01-01'), 1))
    AND l_discount BETWEEN (0.06 - 0.01) AND (0.06 + 0.01)
    AND l_quantity < 24
;
