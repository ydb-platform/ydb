/* syntax version 1 */
USE plato;

SELECT
    value, TopFreq(AsStruct(key as k, value as v), 3, 5u)
FROM Input
GROUP BY value
ORDER BY value