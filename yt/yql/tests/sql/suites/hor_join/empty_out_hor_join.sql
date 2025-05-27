/* postgres can not */

USE plato;

$i = (select * from Input where key == "0" order by key limit 100);

select key, some(value) from $i group by key;
select key, some(subkey) from $i group by key;
