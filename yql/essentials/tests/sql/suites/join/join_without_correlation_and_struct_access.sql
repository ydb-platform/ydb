PRAGMA DisableSimpleColumns;
/* postgres can not */
use plato;

$data = ( select
    cast(key as uint32) % 10 as mod,
    (key as kk, subkey as sk) as struct_field
from Input);

--INSERT INTO Output
SELECT
    mod,
    struct_field.kk as mod_key,
    key,
    value
FROM Input JOIN $data AS d on cast(Input.key as uint32) / 100 == d.mod
order by key, mod_key, value
;
