/* custom error: distinct over window is not supported */
PRAGMA YqlSelect = 'force';

SELECT
    a,
    Sum(DISTINCT b) OVER (
        ORDER BY
            c
    )
FROM (
    VALUES
        (1, 1, 3),
        (2, 1, 2),
        (2, 1, 2),
        (3, 1, 1),
        (3, 1, 1),
        (3, 1, 1)
) AS x (
    a,
    b,
    c
);
