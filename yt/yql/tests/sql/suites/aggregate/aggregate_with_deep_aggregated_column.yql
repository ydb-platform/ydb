/* syntax version 1 */
/* postgres can not */
use plato;

$data = (select cast(key as uint32) as age, cast(subkey as uint32) as region, value as name from Input);

--insert into Output
select
  region,
  max(case when age % 10u between 1u and region % 10u then age else 0u end) as max_age_at_range_intersect
from $data
group by region
order by region
;
