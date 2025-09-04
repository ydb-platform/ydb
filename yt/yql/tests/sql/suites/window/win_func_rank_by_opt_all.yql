/* postgres can not */
/* syntax version 1 */
use plato;
PRAGMA DisableAnsiRankForNullableKeys;

$input=(select cast(key as int32) as key, cast(subkey as int32) as subkey, value from Input);

SELECT
  rank(key) over w1 as rank_key,
  dense_rank(key) over w1 as dense_rank_key,
  key
FROM $input
WINDOW w1 as (ORDER BY key);
