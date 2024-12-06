/* postgres can not */
use plato;

$data = ( select
    cast(key as uint32) % 10 as mod,
    (key as kk, subkey as sk) as struct_field,
    value
from Input);

--INSERT INTO Output
SELECT
    mod,
    struct_field.kk as mod_key,
    struct_field.sk,
    d.value
FROM $data as d
order by mod_key, value
;
