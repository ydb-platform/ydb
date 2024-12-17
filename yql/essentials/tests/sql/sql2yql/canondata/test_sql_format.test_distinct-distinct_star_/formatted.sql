/* syntax version 1 */
USE plato;

SELECT DISTINCT
    *
FROM
    Input2
ORDER BY
    key,
    subkey
;

SELECT DISTINCT
    *
WITHOUT
    subkey
FROM
    Input2
ORDER BY
    key,
    value
;

SELECT DISTINCT
    a.*,
    TableName() AS tn,
WITHOUT
    subkey
FROM
    Input2 AS a
ORDER BY
    key,
    value
;
