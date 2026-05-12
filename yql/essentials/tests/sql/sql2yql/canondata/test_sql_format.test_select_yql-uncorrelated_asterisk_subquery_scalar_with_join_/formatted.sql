PRAGMA YqlSelect = 'force';

SELECT
    (
        SELECT
            *
        FROM (
            SELECT
                1 AS a
        ) AS x
    )
FROM (
    VALUES
        (2)
) AS y (
    b
)
JOIN (
    VALUES
        (3)
) AS z (
    c
)
ON
    y.b == z.c - 1
;
