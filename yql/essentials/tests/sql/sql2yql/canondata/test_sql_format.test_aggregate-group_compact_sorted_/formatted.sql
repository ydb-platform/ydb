/* syntax version 1 */
/* postgres can not */
USE plato;

SELECT
    key,
    count(value) AS cnt,
    min(value) AS min,
    max(value) AS max
FROM Input
GROUP COMPACT BY
    key
ORDER BY
    key;
