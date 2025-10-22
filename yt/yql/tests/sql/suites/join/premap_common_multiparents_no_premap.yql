PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;
use plato;

$map = (select key, subkey, 1 as value, 2 as another from Input1);

from $map as a
join Input2 as b using(key)
select a.key, a.value, b.value
order by a.key,a.value;

from $map as a
select a.key, a.value, a.subkey
order by a.key,a.value;
