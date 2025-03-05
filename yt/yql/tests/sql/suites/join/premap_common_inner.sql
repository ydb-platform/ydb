PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;
use plato;

from (select key, subkey, 1 as value from Input1) as a
join Input2 as b using(key)
select a.key, a.subkey, a.value, b.value
order by a.key;
