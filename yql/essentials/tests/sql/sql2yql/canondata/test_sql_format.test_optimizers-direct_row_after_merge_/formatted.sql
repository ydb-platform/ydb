/* postgres can not */
USE plato;

SELECT
    key,
    TablePath() AS path
FROM concat(Input1, Input2)
ORDER BY
    key,
    path;
