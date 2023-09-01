/* syntax version 1 */
USE plato;

SELECT
    key,
    BOTTOM(subkey, 4u)
FROM Input
GROUP BY key
ORDER BY key
