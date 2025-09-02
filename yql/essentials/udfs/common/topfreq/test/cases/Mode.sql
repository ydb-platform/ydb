/* syntax version 1 */
USE plato;

SELECT
    key, Mode(value)
FROM
(SELECT
   cast (key as Int32) as value,
    "" as subkey,
    value as key
FROM Input)
AS tmp
GROUP BY key
ORDER BY key
