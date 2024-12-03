/* syntax version 1 */
/* postgres can not */
PRAGMA AnsiCurrentRow;

SELECT
    value,
    key,
    subkey,
    SUM(CAST(subkey AS Int32)) OVER w AS subkey_sum_ansi,
    SUM(CAST(subkey AS Int32)) OVER w1 AS subkey_sum,
    SUM(CAST(subkey AS Int32)) OVER w2 AS subkey_sum_next,
FROM plato.Input
WINDOW
    w AS (
        PARTITION BY
            value
        ORDER BY
            key
    ),
    w1 AS (
        PARTITION BY
            value
        ORDER BY
            key
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ),
    w2 AS (
        PARTITION BY
            value
        ORDER BY
            key
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING
    )
ORDER BY
    value,
    key,
    subkey;
