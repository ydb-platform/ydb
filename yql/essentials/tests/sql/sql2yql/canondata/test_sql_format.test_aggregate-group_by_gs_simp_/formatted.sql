/* syntax version 1 */
/* postgres can not */
SELECT
    sum(length(value)),
    key,
    subkey,
    grouping(key, subkey)
FROM
    plato.Input
GROUP BY
    GROUPING SETS (
        (key, subkey),
        key,
        subkey
    )
ORDER BY
    key,
    subkey
;
