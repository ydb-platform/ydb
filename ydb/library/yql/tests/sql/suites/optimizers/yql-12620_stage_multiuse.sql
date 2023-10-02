/* postgres can not */
/* syntax version 1 */
use plato;
$a=select * from Input;
select count(*) from $a;
select count(*) from $a where key != '075';
select * from $a where key != '075';