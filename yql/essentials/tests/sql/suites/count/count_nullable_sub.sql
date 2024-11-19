select
    count(val) as subkey,
    cast(avg(val) as int) as value,
    value as key
from
    (select case key when '0' then NULL else cast(subkey as int) / cast(key as int) end as val, value from plato.Input2) as res
group by value
order by value;