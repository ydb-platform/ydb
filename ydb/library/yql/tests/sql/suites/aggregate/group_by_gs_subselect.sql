/* syntax version 1 */
/* postgres can not */
USE plato;

$sub = (SELECT
  Sum(Cast(subkey as Uint32)) as sumLen,
  key,
  value,
  Grouping(key, value) as grouping
FROM Input
GROUP BY GROUPING SETS ((key),(value))
);

--INSERT INTO Output
SELECT t.sumLen, t.key, t.value, t.grouping FROM $sub as t
ORDER BY t.key, t.value