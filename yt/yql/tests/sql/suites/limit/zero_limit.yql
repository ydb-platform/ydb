/* postgres can not */
use plato;

$x = (select * from Input order by value limit 10);

select * from $x where key > "000" limit coalesce(cast(0.1 * 0 as Uint64), 0);
