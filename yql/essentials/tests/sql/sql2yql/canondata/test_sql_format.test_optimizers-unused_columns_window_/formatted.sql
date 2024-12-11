USE plato;

SELECT
    a,
    lag(a) OVER w AS prev_a,
    min(a) OVER w AS min_a
FROM
    Input
WINDOW
    w AS (
        PARTITION BY
            b
        ORDER BY
            c
    )
ORDER BY
    a
;
