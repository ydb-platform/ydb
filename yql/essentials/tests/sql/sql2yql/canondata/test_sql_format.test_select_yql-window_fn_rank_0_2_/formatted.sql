PRAGMA YqlSelect = 'force';

SELECT
    a,
    Rank() OVER (
        PARTITION BY
            c
        ORDER BY
            a,
            b
    )
FROM (
    VALUES
        (1, 1, 1),
        (1, 1, 1),
        (2, 2, 1),
        (3, 3, 2)
) AS x (
    a,
    b,
    c
);
