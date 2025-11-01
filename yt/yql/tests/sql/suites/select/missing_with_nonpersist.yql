USE plato;

$data = select
    Yson::Parse(cast(key as Yson)) as key,
    text, -- missing colums
    subkey
from Input;

select key, subkey
from $data;
