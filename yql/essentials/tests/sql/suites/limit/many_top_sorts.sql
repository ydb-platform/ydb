/* postgres can not */
/* ignore runonopt ast diff */
/* ignore runonopt plan diff */
use plato;

$i = (select * from Input where key > "100");

select * from $i order by key limit 1;

select * from $i order by subkey limit 2;