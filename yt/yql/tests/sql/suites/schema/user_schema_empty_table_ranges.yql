/* syntax version 1 */
use plato;

select *
from range("","foo","foo")
with schema Struct<Key:String>;

select *
from Range_strict("","foo","foo")
with schema Struct<Key:String>;

select * from Each(ListCreate(String))
with schema Struct<Key:Int32>;

select * from Each_strict(ListCreate(String))
with schema Struct<Key:Int32>;
