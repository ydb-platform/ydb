USE plato;

$i = select
    key,
    AsTuple(key, subkey) as t,
    "value:" || value as value
from Input;

select distinct t from $i order by t;

select * from $i where key == "020";
