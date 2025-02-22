PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;
use plato;

$one = (select key, 1 as subkey, value from Input1);
$two = (select key, value from Input2);
$three = (select key, value, 2 as subkey from Input3);

from $one as a
cross join $two as b
left join $three as c on (c.key = a.key and c.value = b.value)
select * order by a.key,a.subkey,b.key,b.value

