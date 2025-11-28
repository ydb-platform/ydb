/* syntax version 1 */
/* postgres can not */
USE plato;

SELECT
    key, count(value) as cnt, min(value) as min, max(value) as max
FROM Input
GROUP COMPACT BY key
ORDER BY key;

