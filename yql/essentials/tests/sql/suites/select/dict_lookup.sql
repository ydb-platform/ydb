/* syntax version 1 */
/* postgres can not */
use plato;
$dictList = (select
    AsDict(AsTuple(value, CAST(subkey AS Int32))) as `dict`,
    AsDict(AsTuple("z", "a"), AsTuple("y", "b")) AS d,
    subkey, value
from Input);

select d["z"] as static, input.`dict`[input.value] as dynamic, input.`dict` as `dict` from $dictList as input;
