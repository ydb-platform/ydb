/* postgres can not */
/* syntax version 1 */
use plato;

$data = (select cast(key as uint32) as age, cast(subkey as uint32) as region, value as name from Input);

-- insert into Output
$data2 = (select
    region,
    name,
    avg(CAST(age as Interval)) over w1 as avg_age,
from $data
window w1 as (partition by region order by name desc)
);

discard select
  EnsureType(avg_age, Interval?) as avg_age
from $data2
;
