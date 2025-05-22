/* syntax version 1 */
/* postgres can not */
USE plato;

$sub = (SELECT * FROM Input LIMIT 5);

--INSERT INTO Output
SELECT
  Sum(Cast(subkey as Uint32)) as sumLen,
  key,
  value,
  Grouping(key, value) as grouping
FROM $sub
GROUP BY GROUPING SETS ((key),(value))
ORDER BY key, value
