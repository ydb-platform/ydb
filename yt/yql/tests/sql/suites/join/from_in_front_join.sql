PRAGMA DisableSimpleColumns;
/* postgres can not */
from plato.Input1 inner join plato.Input3 using (key) select Input1.key, Input1.subkey, Input3.value;