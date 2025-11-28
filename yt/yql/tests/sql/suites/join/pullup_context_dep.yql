PRAGMA DisableSimpleColumns;
use plato;

from (select key, subkey || key as subkey, value, TablePath() as tp from Input1) as a
join Input2 as b using(key)
select a.key, a.subkey, a.tp, b.value
order by a.key, a.tp;
