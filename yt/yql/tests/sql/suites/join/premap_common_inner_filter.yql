PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;
use plato;

from (select key, subkey || key as subkey, value from Input1 where value != "ddd") as a
join Input2 as b using(key)
select a.key, a.subkey, b.value
order by a.key;
