/* syntax version 1 */
USE plato;

SELECT
    key, TopFreq(value, 3, 5u)
FROM
(SELECT
   key as value,
    "" as subkey,
    cast (value as Uint32) as key
FROM Input)
AS tmp
GROUP BY key
ORDER BY key
