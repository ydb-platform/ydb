/* syntax version 1 */
/* postgres can not */
use plato;

--insert into Output
select
  key, subkey, count(*) as total_count
from plato.Input
where key in ('023', '037')
group by rollup(key, subkey)
order by key, subkey
;
