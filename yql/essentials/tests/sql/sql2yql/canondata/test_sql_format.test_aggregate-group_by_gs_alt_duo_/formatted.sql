/* syntax version 1 */
/* postgres can not */
SELECT
    sum(length(value)),
    key,
    subkey
FROM
    plato.Input
GROUP BY
    GROUPING SETS (
        key
    ),
    GROUPING SETS (
        subkey
    )
ORDER BY
    key,
    subkey
;
