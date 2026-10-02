PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;
use plato;

from (select key, subkey, 1 as value from Input1) as a
join (select key, subkey, 2 as value from Input2) as b using(key)
select a.key as key, a.subkey, a.value, b.subkey, b.value
order by key;
