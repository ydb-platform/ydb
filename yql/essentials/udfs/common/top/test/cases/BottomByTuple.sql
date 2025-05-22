/* syntax version 1 */
USE plato;

SELECT
    key,
    BOTTOM_BY(AsTuple(subkey, value), AsTuple(subkey, value), 10u)
FROM Input
GROUP BY key
ORDER BY key
