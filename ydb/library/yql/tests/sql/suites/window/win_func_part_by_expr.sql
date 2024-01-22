/* postgres can not */
use plato;

$data = (select cast(key as uint32) as age, cast(subkey as uint32) as region, value as name from Input);

--insert into Output
select
  prefix,
  region,
  name,
  sum(age) over w1 as sum1
from $data
window w1 as (partition by SUBSTRING(name,0,1) as prefix order by name)
order by prefix, region, name;
