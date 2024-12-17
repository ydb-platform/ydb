USE plato;

SELECT
    key,
    RANK() OVER w
FROM
    Input
GROUP BY
    key
WINDOW
    w AS (
        ORDER BY
            key
    )
ORDER BY
    key
;
