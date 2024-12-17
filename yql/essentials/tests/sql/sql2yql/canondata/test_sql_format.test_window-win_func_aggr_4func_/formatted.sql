/* postgres can not */
SELECT
    subkey,
    2 * sum(CAST(key AS uint32)) OVER w1 AS dbl_sum,
    count(key) OVER w1 AS c,
    min(key) OVER w1 AS mink,
    max(key) OVER w1 AS maxk
FROM
    plato.Input
WINDOW
    w1 AS (
        PARTITION BY
            subkey
        ORDER BY
            key
    )
ORDER BY
    subkey,
    c
;
