/* postgres can not */
/* syntax version 1 */
use plato;

$zero = unwrap( cast(0 as Interval) );

-- safely cast data to get rid of optionals after cast
$prepared = select
   cast(key as Interval) ?? $zero as age
  , cast(subkey as uint32) as region
  , value as name
  from Input;

-- we want to check both optional<interval> and plain interval
$data = (select
  age
  , just(age) as age_opt
  , region
  , name
  from $prepared);

$data2 = (select
    region,
    name,
    percentile(age, 0.8) over w1 as age_p80,
    percentile(age_opt, 0.8) over w1 as age_opt_p80,
from $data
window w1 as (partition by region order by name desc)
);

select
  EnsureType(age_p80, Interval) as age_p80
  , EnsureType(age_opt_p80, Interval?) as age_opt_p80
from $data2
;

