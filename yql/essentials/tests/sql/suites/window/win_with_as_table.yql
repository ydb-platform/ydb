/* postgres can not */
select k,
  SUM(k) over w1 as s1,
  SUM(k) over w2 as s2
from as_table(AsList(AsStruct(1 as k), AsStruct(2 as k)))
window w1 as (order by k), w2 as (order by k desc)
order by k;
