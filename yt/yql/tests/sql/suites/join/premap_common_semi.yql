PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;
use plato;

from (select key, subkey as asubkey, value from Input1) as a
left semi join (select key, 1 as value from Input2) as b using(key)
select a.key, a.asubkey
order by a.key;
