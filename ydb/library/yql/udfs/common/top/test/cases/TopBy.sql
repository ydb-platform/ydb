/* syntax version 1 */
USE plato;

SELECT
    key,
    TOP_BY(subkey || value, subkey, 6u)
FROM Input
GROUP BY key
ORDER BY key
