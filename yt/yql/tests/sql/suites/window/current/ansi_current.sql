/* syntax version 1 */
/* postgres can not */
pragma AnsiCurrentRow;

SELECT
    value,
    key,
    subkey,
    SUM(cast(subkey as Int32)) over w as subkey_sum,
FROM plato.Input
WINDOW w AS (
    PARTITION BY value
    ORDER BY key
)
ORDER BY value, key, subkey;
