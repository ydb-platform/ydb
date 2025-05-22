/* syntax version 1 */
USE plato;

SELECT
    key,
    TOP(AsTuple(subkey, value), 5u)
FROM Input
GROUP BY key
ORDER BY key