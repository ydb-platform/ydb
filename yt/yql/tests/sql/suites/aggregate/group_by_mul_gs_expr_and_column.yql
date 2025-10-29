/* syntax version 1 */
/* postgres can not */
use plato;

--insert into Output
select
  count(1) as count, kf, key, vf, vl, grouping(kf, key, vf, vl) as grouping
from Input group by grouping sets(
  (cast(key as uint32) / 100u as kf, key),
  (Substring(value, 0, 1) as vf, Substring(value, 2, 1) as vl)
)
order by kf, key, vf, vl;
