/* syntax version 1 */
USE plato;

SELECT
    key,
    BOTTOM_BY(subkey || value, CAST(subkey AS Uint64), 5u)
FROM Input
GROUP BY key
ORDER BY key
