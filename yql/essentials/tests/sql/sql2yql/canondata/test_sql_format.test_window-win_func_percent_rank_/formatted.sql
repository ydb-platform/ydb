SELECT
    r,
    x,
    percent_rank() OVER w,
    percent_rank(x) OVER w,
FROM (
    SELECT
        *
    FROM (
        VALUES
            (1, NULL),
            (2, 3),
            (3, 4),
            (4, 4)
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
