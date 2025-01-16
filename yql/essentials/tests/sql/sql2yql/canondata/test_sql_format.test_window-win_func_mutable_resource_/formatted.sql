/* postgres can not */
SELECT
    median(x) OVER w,
    median(x) OVER w
FROM (
    SELECT
        x,
        0 AS y
    FROM (
        SELECT
            AsList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) AS x
    )
        FLATTEN BY x
)
WINDOW
    w AS (
        ORDER BY
            y
    )
;
