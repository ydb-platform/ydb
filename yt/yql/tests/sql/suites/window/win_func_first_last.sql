/* postgres can not */
SELECT
  key,
  FIRST_VALUE(cast(subkey as uint32)) RESPECT NULLS OVER w1,
  FIRST_VALUE(cast(subkey as uint32)) IGNORE NULLS OVER w1,
  LAST_VALUE(cast(subkey as uint32)) OVER w1,
  LAST_VALUE(cast(subkey as uint32)) IGNORE NULLS OVER w1,
  subkey
FROM plato.Input
WINDOW w1 as (ORDER BY key desc, subkey);
