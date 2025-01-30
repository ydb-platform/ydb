/* postgres can not */
select
  2 * sum(cast(key as uint32)) over w1 as dbl_sum,
  count(key) over w1 as c,
  min(key) over w1 as mink,
  max(key) over w1 as maxk
from plato.Input
window w1 as (order by key)
order by c;
