/* postgres can not */
/* multirun can not */
/* syntax version 1 */
use plato;

$list = ListCreate(Struct<key:String, subkey:String, value:String>);

insert into Output
select * from as_table($list) order by key;
