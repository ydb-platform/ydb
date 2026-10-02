PRAGMA YqlSelect = 'force';

SELECT
    a,
    Count(b)
FROM (
    VALUES
        (1, 110),
        (2, 210),
        (2, 220),
        (3, 310),
        (3, 320),
        (3, 330)
) AS x (
    a,
    b
)
WHERE
    2 <= a AND a <= 3
    AND (b % 100) != 10
GROUP BY
    a
;
