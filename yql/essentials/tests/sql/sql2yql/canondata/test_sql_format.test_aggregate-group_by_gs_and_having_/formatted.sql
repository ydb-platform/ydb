/* syntax version 1 */
/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
    Sum(CAST(subkey AS Uint32)) AS sumLen,
    key,
    value,
    Grouping(key, value) AS grouping
FROM Input3
GROUP BY
    GROUPING SETS (
        (key),
        (value))
HAVING count(*) > 2
ORDER BY
    key,
    value;
