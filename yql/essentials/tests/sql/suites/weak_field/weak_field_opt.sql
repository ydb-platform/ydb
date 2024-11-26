/* postgres can not */
use plato;
select subkey, WeakField(data3, "int32") as data3str, WeakField(datahole3, "int32", 999) as holes3 from Input3 order by subkey
