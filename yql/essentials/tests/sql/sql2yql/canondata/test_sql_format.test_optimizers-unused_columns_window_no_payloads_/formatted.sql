USE plato;

SELECT
    b
FROM (
    SELECT
        b,
        lag(a) OVER w AS prev_a
    FROM
        Input
    WINDOW
        w AS (
            PARTITION BY
                b
            ORDER BY
                c
        )
);
