/* syntax version 1 */
/* postgres can not */

SELECT
    key,
    subkey,
    value,

    AGGREGATE_LIST(TableRow()) OVER w AS frame,

FROM plato.Input
WINDOW
    w as (PARTITION BY key, subkey ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
ORDER BY value;
