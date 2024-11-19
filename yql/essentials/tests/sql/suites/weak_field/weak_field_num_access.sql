/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
  subkey,
  WeakField(data3, "int32") as data3,
  WeakField(datahole3, "uint32", 999) as holes3
FROM Input4
ORDER BY subkey
