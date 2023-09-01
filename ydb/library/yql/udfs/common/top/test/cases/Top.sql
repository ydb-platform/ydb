/* syntax version 1 */
USE plato;

SELECT
    key,
    TOP(CAST(subkey AS Float), 3u)
FROM Input
GROUP BY key
ORDER BY key
