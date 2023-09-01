/* syntax version 1 */
USE plato;

SELECT
    key,
    AGGREGATE_LIST_DISTINCT(value, 3)
FROM Input
GROUP BY key
ORDER BY key
