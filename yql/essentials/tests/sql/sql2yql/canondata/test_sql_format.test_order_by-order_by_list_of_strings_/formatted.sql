/* postgres can not */
USE plato;

SELECT
    key,
    value
FROM
    Input
ORDER BY
    value
LIMIT 1;
