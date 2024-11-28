/* postgres can not */
select
  subkey,
  sum(cast(key as uint32)) over w1 as s,
  count(key) over w1 as c,
  min(key) over w1 as mink,
  max(key) over w1 as maxk
from plato.Input window w1 as (partition by subkey order by key)
order by subkey, c;
