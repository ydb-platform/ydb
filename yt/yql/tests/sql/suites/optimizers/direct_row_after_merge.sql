/* postgres can not */
USE plato;

SELECT
    key,
    TablePath() as path
FROM concat(Input1, Input2)
order by key, path;