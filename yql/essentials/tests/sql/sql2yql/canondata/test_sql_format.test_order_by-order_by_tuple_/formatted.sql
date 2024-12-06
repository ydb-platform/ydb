/* postgres can not */
USE plato;

SELECT
    *
FROM (
    SELECT
        key,
        AsTuple(key, subkey) AS tpl
    FROM Input
)
ORDER BY
    tpl;
