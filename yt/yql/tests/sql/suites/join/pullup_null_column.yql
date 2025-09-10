PRAGMA DisableSimpleColumns;
use plato;

from Input1 as a
left join (select key, null as subkey, 1 as value from Input2) as b on a.key = b.key
select a.key as akey, b.key as bkey, a.subkey, b.subkey, b.value
order by akey;
