/* postgres can not */
use plato;
select
    WeakField(key, DataType("String")),
    WeakField(subkey, OptionalType(DataType("String"))),
    WeakField(value, "String")
from Input;
