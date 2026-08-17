PRAGMA YqlSelect = 'force';

SELECT
    a,
    Lag(a) OVER (
        PARTITION BY
            c
        ORDER BY
            a
    )
FROM (
    VALUES
        (1, 1, 1),
        (2, 2, 1),
        (3, 3, 1),
        (4, 4, 2)
) AS x (
    a,
    b,
    c
);
