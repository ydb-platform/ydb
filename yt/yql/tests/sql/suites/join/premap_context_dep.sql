PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;
use plato;

from (select key, subkey || key as subkey, value, TableRecordIndex() as tr from Input1) as a
join Input2 as b using(key)
select a.key, a.subkey, a.tr, b.value
order by a.key, a.tr;
