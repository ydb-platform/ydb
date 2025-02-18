PRAGMA DisableSimpleColumns;
select Input1.key, Input1.subkey, Input3.value
from plato.Input1
left join plato.Input3 using (key)
order by Input1.key;