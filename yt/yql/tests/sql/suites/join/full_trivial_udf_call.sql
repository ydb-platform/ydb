PRAGMA DisableSimpleColumns;
/* postgres can not */
select Input1.key, Input1.subkey, SimpleUdf::Concat(coalesce(Input1.value, ""), coalesce(Input3.value, "")) as value
from plato.Input1
full join plato.Input3 using (key)
order by Input1.key, Input1.subkey, value;