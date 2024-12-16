/* postgres can not */
USE plato;

SELECT
    *
FROM
    Input
ORDER BY
    TablePath(),
    key
;
