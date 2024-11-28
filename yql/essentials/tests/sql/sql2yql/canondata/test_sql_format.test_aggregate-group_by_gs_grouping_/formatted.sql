/* syntax version 1 */
/* postgres can not */
SELECT
    count(1),
    key_first,
    val_first,
    grouping(key_first, val_first) AS group
FROM plato.Input
GROUP BY
    GROUPING SETS (
        CAST(key AS uint32) / 100u AS key_first,
        Substring(value, 1, 1) AS val_first)
ORDER BY
    key_first,
    val_first;
