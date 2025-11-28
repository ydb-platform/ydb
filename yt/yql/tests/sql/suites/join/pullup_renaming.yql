PRAGMA DisableSimpleColumns;
use plato;

from (select key as akey1, key as akey2, subkey || key as subkey, value from Input1) as a
left join (select key || subkey as subkey, key as bkey1, key as bkey2, 1 as value from Input2) as b
on a.akey1 = b.bkey1 and a.akey1 = b.bkey2 and a.akey2 = b.bkey1
select a.akey1 as akey1, b.bkey1 as bkey1, a.subkey, b.subkey, b.value
order by akey1, bkey1;
