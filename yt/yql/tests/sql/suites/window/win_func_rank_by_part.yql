/* postgres can not */
/* syntax version 1 */
use plato;
PRAGMA DisableAnsiRankForNullableKeys;

$input=(select cast(key as int32) % 4 as key_quad, cast(key as int32) ?? 0 as key, cast(subkey as int32) as subkey, value from Input);

SELECT
  rank(key) over w1 as rank_key,
  dense_rank(key) over w1 as dense_rank_key,
  key_quad,
  key
FROM $input
WINDOW w1 as (PARTITION BY key_quad ORDER BY key)
ORDER BY rank_key, dense_rank_key, key_quad;
