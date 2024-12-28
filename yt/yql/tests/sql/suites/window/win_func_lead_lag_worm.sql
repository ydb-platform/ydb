/* postgres can not */
use plato;

$input=(select cast(key as int32) as key, cast(subkey as int32) as subkey, value from Input);

--insert into Output
select
  lead(key, 3) over w as keyL,
  lead(key, 2) over w as keyM,
  lead(key, 1) over w as keyN,
  key as keyO,
  lag(key, 1) over w as key_1,
  lag(key, 2) over w as key_2,
  lag(key, 3) over w as key_3,
  lag(key, 4) over w as key_4,
  lag(key, 5) over w as key_5,
  value
from $input
window w as (order by key, value)
order by keyO, value
;
