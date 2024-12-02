/* syntax version 1 */
/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
    key,
    count(1) AS count
FROM Input
GROUP BY
    CAST(key AS uint32) % 10 AS key
ORDER BY
    key,
    count;
