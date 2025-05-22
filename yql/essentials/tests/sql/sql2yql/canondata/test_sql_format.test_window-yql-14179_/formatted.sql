SELECT
    x,
    aggregate_list(x) OVER w AS lst,
FROM (
    VALUES
        (1),
        (2),
        (3)
) AS a (
    x
)
WINDOW
    w AS (
        ROWS BETWEEN 0 PRECEDING AND 0 PRECEDING
    )
ORDER BY
    x
;
