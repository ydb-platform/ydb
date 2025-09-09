/* syntax version 1 */
/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
  Sum(Cast(subkey as Uint32)) as sumLen,
  key,
  value,
  Grouping(key, value) as grouping
FROM Input3
GROUP BY GROUPING SETS ((key),(value))
HAVING count(*) > 2
ORDER BY key, value
