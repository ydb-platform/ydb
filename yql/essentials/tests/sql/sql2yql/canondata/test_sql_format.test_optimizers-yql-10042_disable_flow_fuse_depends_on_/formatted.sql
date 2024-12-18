/* postgres can not */
/* syntax version 1 */
USE plato;

SELECT
    RandomNumber(a) AS first,
    RandomNumber(b) AS second
FROM (
    SELECT
        key AS a,
        key AS b
    FROM
        Input0
)
ORDER BY
    first,
    second
;
