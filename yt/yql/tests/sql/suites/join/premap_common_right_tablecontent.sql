PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;
use plato;


$keys = (select key from Input3);

from (select key, value, key IN $keys as flag from Input1) as a
right join (select key, key IN $keys as flag from Input2) as b using(key)
select a.key, a.value, a.flag, b.flag
order by a.key;
