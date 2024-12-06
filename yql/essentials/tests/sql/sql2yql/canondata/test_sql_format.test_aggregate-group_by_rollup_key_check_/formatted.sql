/* syntax version 1 */
/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
    key,
    prefix,
    COUNT(*) AS cnt,
    grouping(key, prefix) AS agrouping
FROM Input
GROUP BY
    ROLLUP (key AS key, Substring(value, 1, 1) AS prefix)
ORDER BY
    key,
    prefix;
