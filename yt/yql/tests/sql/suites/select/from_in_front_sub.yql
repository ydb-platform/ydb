/* postgres can not */
from
    (select cast(subkey as Double) / cast(key as Double) as val, value from plato.Input2) as res
select
    count(val) as subkey,
    cast(avg(val) as int) as value,
    value as key
group by value
order by subkey, value;