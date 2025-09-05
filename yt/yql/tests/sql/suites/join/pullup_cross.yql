PRAGMA DisableSimpleColumns;
use plato;

from (select key, subkey || key as subkey, value || "v" as value from Input1) as a
cross join Input2 as b
select a.key, a.subkey, b.subkey, b.value
order by a.key, a.subkey, b.subkey, b.value;
