PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;
use plato;

-- not renaming
from (select key, subkey || key as subkey, value from Input1) as a
join Input2 as b using(key)
select a.key, a.subkey, b.value
order by a.key;

-- not fixed size
from (select key, "aaa" as subkey, value from Input1) as a
join Input2 as b using(key)
select a.key, a.subkey, b.value
order by a.key;

-- to many Justs
from (select key, Just(Just(1)) as subkey, value from Input1) as a
join Input2 as b using(key)
select a.key, a.subkey, b.value
order by a.key;

-- container
from (select key, AsTuple(1) as subkey, value from Input1) as a
join Input2 as b using(key)
select a.key, a.subkey, b.value
order by a.key;
