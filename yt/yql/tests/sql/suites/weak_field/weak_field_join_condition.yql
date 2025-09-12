/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
  i1.subkey as sk,
  WeakField(i1.value1, "String", "funny") as i1v1,
  WeakField(i1.value2, "String", "bunny") as i1v2,
  WeakField(i2.value1, "String", "short") as i2v1,
  WeakField(i2.value2, "String", "circuit") as i2v2
FROM Input1 as i1 JOIN Input2 as i2 ON WeakField(i1.value1, "String") == WeakField(i2.value2, "String") ORDER BY sk
