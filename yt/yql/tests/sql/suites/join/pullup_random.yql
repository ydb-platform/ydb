/* kikimr can not - due to random */
PRAGMA DisableSimpleColumns;
use plato;

from (select key, subkey || key as subkey, value, RANDOM(value || "x") <= 1.0 as rn from Input1) as a
join Input2 as b using(key)
select a.key, a.subkey, a.rn, b.value
order by a.key, a.rn;
