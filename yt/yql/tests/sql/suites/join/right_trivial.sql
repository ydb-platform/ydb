PRAGMA DisableSimpleColumns;
select coalesce(Input1.key, "_null") as a, coalesce(Input1.subkey, "_null") as b, coalesce(Input3.value, "_null") as c
from plato.Input1
right join plato.Input3 using (key)
order by a, b, c;