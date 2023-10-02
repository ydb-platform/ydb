/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
  odd,
  sum(WeakField(data3, "int32") + WeakField(datahole3, "uint32", 999)) as score
FROM Input4
GROUP BY cast(subkey as uint32) % 2 as odd
ORDER BY odd, score
