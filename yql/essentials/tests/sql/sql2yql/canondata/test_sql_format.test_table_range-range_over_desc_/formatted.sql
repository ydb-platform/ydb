/* postgres can not */
/* kikimr can not - range not supported */
/* syntax version 1 */
USE plato;

SELECT
    t.*,
    TableName() AS path
FROM
    range("", "Input1", "Input2") AS t
ORDER BY
    path,
    key,
    value
;
