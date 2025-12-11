PRAGMA YqlSelect = 'force';

SELECT
    a,
    b,
    Count(*)
FROM (
    VALUES
        (1, 11, 110),
        (2, 22, 210),
        (2, 22, 220),
        (3, 33, 310),
        (3, 33, 320),
        (3, 33, 330)
) AS x (
    a,
    b,
    c
)
GROUP BY
    a,
    b
;
