/* syntax version 1 */
/* postgres can not */
use plato;

select
    key,
    Yson::LookupString(subkey, "a") as a,
from Input with ignore_type_v3;