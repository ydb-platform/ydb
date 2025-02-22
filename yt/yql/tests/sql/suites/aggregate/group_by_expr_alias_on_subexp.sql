/* syntax version 1 */
/* postgres can not */
USE plato;

--INSERT INTO Output
SELECT
  key, sk, aggregate_list(value) as values
FROM
  (SELECT * FROM Input)
GROUP BY key, cast(subkey as uint32) % 2 as sk
ORDER BY key, sk;
