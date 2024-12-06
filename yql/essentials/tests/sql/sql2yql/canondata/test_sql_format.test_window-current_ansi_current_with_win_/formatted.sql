/* syntax version 1 */
/* postgres can not */
PRAGMA AnsiCurrentRow;

SELECT
    value,
    key,
    subkey,
    SUM(CAST(subkey AS Int32)) OVER w AS subkey_sum_ansi,
    LEAD(CAST(subkey AS Int32)) OVER w AS subkey_next,
FROM plato.Input
WINDOW
    w AS (
        PARTITION BY
            value
        ORDER BY
            key
    )
ORDER BY
    value,
    key,
    subkey;
