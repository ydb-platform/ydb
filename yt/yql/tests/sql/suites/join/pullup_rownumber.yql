PRAGMA DisableSimpleColumns;
use plato;

from (select key, subkey || key as subkey, value, ROW_NUMBER() over w as rn from Input1 window w as ()) as a
join Input2 as b using(key)
select a.key, a.subkey, a.rn, b.value
order by a.key, a.rn;
