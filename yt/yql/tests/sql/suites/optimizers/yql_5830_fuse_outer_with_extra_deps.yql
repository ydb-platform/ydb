/* postgres can not */
USE plato;

$data = (select max_by(key, subkey)
from Input where value > "a");

select
 a.key,
 $data as max_key,
 b.value
from Input as a
left join (select * from Input where key > "050") as b
on a.key = b.key
order by a.key;
