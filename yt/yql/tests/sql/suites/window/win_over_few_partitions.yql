/* postgres can not */
$data = (select
    cast(key as uint32) as age,
    cast(key as uint32)/10 as age_decade,
    cast(subkey as uint32) as region,
    value as name
from plato.Input);

select
  region, age, name, sum(age) over w1 as sum1, sum(age) over w2 as sum2
from $data
window
  w1 as (partition by region order by name),
  w2 as (partition by age_decade order by name)
order by region, age;
