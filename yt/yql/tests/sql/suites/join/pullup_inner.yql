PRAGMA DisableSimpleColumns;
use plato;

from (select key, subkey || key as subkey, value from Input1) as a
join (select key || subkey as subkey, key, 1 as value from Input2) as b on a.key = b.key
select a.key as akey, b.key as bkey, a.subkey, b.subkey, b.value
order by akey;
