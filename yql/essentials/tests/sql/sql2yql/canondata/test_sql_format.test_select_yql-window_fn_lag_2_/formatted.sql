PRAGMA YqlSelect = 'force';

SELECT
    a,
    Lag(a, 2) OVER (
        PARTITION BY
            c
        ORDER BY
            a
    )
FROM (
    VALUES
        (10, 0, 1),
        (20, 0, 1),
        (30, 0, 1),
        (40, 0, 1),
        (50, 0, 1),
        (100, 0, 2)
) AS x (
    a,
    b,
    c
);
