PRAGMA DisableSimpleColumns;
select
    Input1.key as key,
    Input1.subkey as subkey,
    selected.value as value
from plato.Input1
inner join (select key, value || value as value from plato.Input3) as selected
    using (key)
order by key desc;