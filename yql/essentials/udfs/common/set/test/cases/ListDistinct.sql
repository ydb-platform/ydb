/* syntax version 1 */
USE plato;

SELECT
    key,
    AGGREGATE_LIST_DISTINCT(value)
FROM Input
GROUP BY key
ORDER BY key
