PRAGMA YqlSelect = 'force';

SELECT
    a,
    Sum(b) OVER (
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
