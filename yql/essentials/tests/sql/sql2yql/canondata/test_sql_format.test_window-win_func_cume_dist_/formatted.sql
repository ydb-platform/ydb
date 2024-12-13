SELECT
    r,
    x,
    cume_dist() OVER w,
FROM (
    SELECT
        *
    FROM (
        VALUES
            (1, 3),
            (2, NULL),
            (3, 4),
            (4, 5)
    ) AS a (
        r,
        x
    )
) AS z
WINDOW
    w AS (
        ORDER BY
            r
    )
ORDER BY
    r
;
