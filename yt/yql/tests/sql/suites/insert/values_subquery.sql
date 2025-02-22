/* postgres can not */
use plato;

$a = (select key from Input order by key limit 1);

insert into Output (key) values ($a);
