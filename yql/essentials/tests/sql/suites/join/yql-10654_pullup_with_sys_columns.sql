/* postgres can not */
/* syntax version 1 */
USE plato;
PRAGMA DisableSimpleColumns;

$src =
    select key, subkey || key as subkey, value from Input
    union all
    select * from AS_TABLE(ListCreate(Struct<key:String,subkey:String,value:String>));

select a.key, a.subkey, b.value
from Input as a
left join $src as b using(key)
order by a.key;
