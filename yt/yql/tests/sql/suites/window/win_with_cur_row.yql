/* postgres can not */
$data = (select cast(key as uint32) as age, cast(subkey as uint32) as region, value as name from plato.Input);

select
  region, name, sum(age) over w1 as sum1
from $data
window w1 as (partition by region order by name)
order by region, name;
