/* postgres can not */
$input=(select cast(key as int32) / 100 as key_hundred, cast(key as int32) as key, cast(subkey as int32) as subkey, value from plato.Input);

select
  key_hundred,
  key,
  (key - lag(key, 1) over w) as key_diff,
  (subkey - lag(subkey, 1) over w) as subkey_diff,
  row_number() over w as row,
  value
from $input
window w as (partition by key_hundred order by key, value)
order by key_hundred, key, value
;
