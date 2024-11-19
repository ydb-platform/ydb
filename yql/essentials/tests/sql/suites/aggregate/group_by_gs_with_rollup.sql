/* syntax version 1 */
/* postgres can not */
use plato;

--insert into Output
select
  key, subkey, count(1) as total_count, value, grouping(key, subkey, value) as group_mask
from Input
group by grouping sets (value, rollup(key, subkey))
order by group_mask, value, key, subkey, total_count;
