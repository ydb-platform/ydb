/* postgres can not */
/* syntax version 1 */
USE plato;

SELECT
    value,
    AGG_LIST_DISTINCT(tpl) AS tuples
FROM (
    SELECT
        AsTuple(key, subkey, value) AS tpl,
        value
    FROM
        InputSorted
)
WHERE
    value IN (
        SELECT DISTINCT
            value
        FROM
            Input
    )
GROUP BY
    value
;
