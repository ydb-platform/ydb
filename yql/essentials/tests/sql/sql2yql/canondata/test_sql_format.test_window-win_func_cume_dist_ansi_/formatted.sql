PRAGMA AnsiCurrentRow;

SELECT
    cume_dist() OVER w
FROM (
    VALUES
        (4),
        (5),
        (5),
        (6)
) AS a (
    x
)
WINDOW
    w AS (
        ORDER BY
            x
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    )
;
