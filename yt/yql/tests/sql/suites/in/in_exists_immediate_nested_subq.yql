/* postgres can not */
/* ignore runonopt plan diff */

use plato;

$q = (select key from Input order by key limit 100);
$q1 = (select * from $q order by key limit 100);

select * from Input where key in (select * from $q order by key limit 100) order by key;
select * from Input where key in $q1                                       order by key;

SELECT EXISTS (select key from $q) from Input;

SELECT $q;
