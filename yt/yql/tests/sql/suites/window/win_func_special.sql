/* postgres can not */
$input=(select cast(key as int32) as key, cast(subkey as int32) as subkey, value from plato.Input);

select
  key,
  (key - lag(key, 1) over w) as key_diff,
  (subkey - lag(subkey, 1) over w) as subkey_diff,
  row_number() over w as row,
  value
from $input
window w as (order by key, subkey, value);
