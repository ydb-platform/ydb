PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;
use plato;

from (select key as akey, subkey, value as avalue from Input1) as a
cross join Input2 as b
select a.akey, a.subkey, b.subkey, b.value
order by a.akey, a.subkey, b.subkey, b.value;
