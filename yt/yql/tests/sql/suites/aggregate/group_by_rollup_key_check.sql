/* syntax version 1 */
/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
    key, prefix,
    COUNT(*) AS cnt,
    grouping(key, prefix) as agrouping
FROM Input
GROUP BY ROLLUP (key as key, Substring(value, 1, 1) as prefix)
ORDER BY key, prefix;
