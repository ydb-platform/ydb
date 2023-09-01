/* syntax version 1 */
USE plato;

SELECT
    value, TopFreq(AsTuple(key, value), 3, 5u)
FROM Input
GROUP BY value
ORDER BY value