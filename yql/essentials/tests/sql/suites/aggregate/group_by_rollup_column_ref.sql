/* syntax version 1 */
/* postgres can not */
use plato;

$input=(select cast(key as int32) ?? 0 as kk, cast(subkey as int32) ?? 0 as sk, value from Input);

--insert into Output
select
  kk, sk, count(*) as total_count
from $input
where sk in (23, 37, 75, 150, )
group by rollup(kk, sk)
order by kk, sk, total_count
;
