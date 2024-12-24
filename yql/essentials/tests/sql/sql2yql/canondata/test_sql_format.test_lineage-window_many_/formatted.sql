INSERT INTO plato.Output
SELECT
    key,
    row_number() OVER w1,
    lag(value) OVER w1,
    lead(value) OVER w1,
    rank(value) OVER w2,
    dense_rank(value) OVER w2
FROM
    plato.Input
WINDOW
    w1 AS (
        ORDER BY
            key
    ),
    w2 AS (
        ORDER BY
            key DESC
    )
;
