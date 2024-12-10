/* postgres can not */
$input = (
    SELECT
        CAST(key AS uint32) AS key,
        CAST(subkey AS uint32) AS subkey,
        value
    FROM
        plato.Input
);

SELECT
    subkey,
    sum(subkey) OVER w2 AS x,
    2 * sum(key) OVER w1 AS dbl_sum,
    count(key) OVER w1 AS c,
    min(key) OVER w1 AS mink,
    max(key) OVER w1 AS maxk
FROM
    $input
WINDOW
    w1 AS (
        PARTITION BY
            subkey
        ORDER BY
            key % 3,
            key
    ),
    w2 AS (
        PARTITION BY
            key
        ORDER BY
            subkey
    )
ORDER BY
    subkey,
    x,
    dbl_sum
;
