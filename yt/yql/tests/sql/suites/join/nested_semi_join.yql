PRAGMA DisableSimpleColumns;
/* postgres can not */
use plato;

$w1 = (select u.subkey as subkey, u.value as value from Input1 as u left semi join Input1 as v on u.key == v.key);

$t1 = (select x.value from Input1 as x left semi join $w1 as y on x.subkey == y.subkey);

select * from $t1;
