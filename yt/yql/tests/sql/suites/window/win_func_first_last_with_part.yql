/* postgres can not */
$input=(select cast(key as int32) / 100 as key_hundred, cast(key as int32) as key, cast(subkey as int32) as subkey, value from plato.Input);

SELECT
  key_hundred,
  key,
  FIRST_VALUE(cast(subkey as uint32)) RESPECT NULLS OVER w1 as first_res_null,
  FIRST_VALUE(cast(subkey as uint32)) IGNORE NULLS OVER w1 as first_esc_null,
  LAST_VALUE(cast(subkey as uint32)) OVER w1 as last_res_null,
  LAST_VALUE(cast(subkey as uint32)) IGNORE NULLS OVER w1 as last_esc_null,
  subkey
FROM $input
WINDOW w1 as (PARTITION BY key_hundred ORDER BY key)
ORDER BY key_hundred, key, subkey;
