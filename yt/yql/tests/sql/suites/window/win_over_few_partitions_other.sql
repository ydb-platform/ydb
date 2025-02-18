/* postgres can not */
$input = (select cast(key as uint32) as key, cast(subkey as uint32) as subkey, value from plato.Input);

select
  subkey,
  sum(subkey) over w2 AS x,
  2 * sum(key) over w1 as dbl_sum,
  count(key) over w1 as c,
  min(key) over w1 as mink,
  max(key) over w1 as maxk
from $input
window
  w1 as (partition by subkey order by key % 3, key),
  w2 as (partition by key order by subkey)
order by subkey, x, dbl_sum;
