/* syntax version 1 */
/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
    key, count(1) as count
FROM Input
GROUP BY cast(key as uint32) % 10 as key
ORDER BY key, count;
