/* postgres can not */
SELECT
    subkey,
    HISTOGRAM(CAST(key AS uint32) % 10, 2.) OVER w1 AS hh,
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
