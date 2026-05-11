PRAGMA YqlSelect = 'force';

SELECT
    a,
    PercentRank(a) OVER (
        PARTITION BY
            c
        ORDER BY
            a DESC
    )
FROM (
    VALUES
        (1, 1, 1),
        (2, 2, 1),
        (3, 3, 1),
        (4, 4, 1),
        (5, 5, 2)
) AS x (
    a,
    b,
    c
);
