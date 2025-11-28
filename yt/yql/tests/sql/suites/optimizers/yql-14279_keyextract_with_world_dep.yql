/* postgres can not */
use plato;

$input = select * from range("", "Input1", "Input2");

$key = select min(key) from $input;

select key, subkey, value
from $input
where subkey > '1' and key > $key
order by key
;
