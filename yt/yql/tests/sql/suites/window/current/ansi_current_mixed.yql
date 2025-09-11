/* syntax version 1 */
/* postgres can not */
pragma AnsiCurrentRow;

SELECT
    value,
    key,
    subkey,
    SUM(cast(subkey as Int32)) over w  as subkey_sum_ansi,
    SUM(cast(subkey as Int32)) over w1 as subkey_sum,
    SUM(cast(subkey as Int32)) over w2 as subkey_sum_next,
FROM plato.Input
WINDOW w AS (
    PARTITION BY value
    ORDER BY key
), w1 AS (
    PARTITION BY value
    ORDER BY key
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
), w2 AS (
    PARTITION BY value
    ORDER BY key
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING
)
ORDER BY value, key, subkey;
