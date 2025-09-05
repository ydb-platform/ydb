/* syntax version 1 */
/* postgres can not */
use plato;

$input=(select cast(key as uint32) ?? 0 as key, cast(subkey as int32) ?? 0 as subkey, value from Input);

--insert into Output
select
  key, subkey, count(*) as total_count
from $input
where subkey in (23, 37, 75,150)
group by rollup(key, subkey)
order by key, subkey, total_count
;
