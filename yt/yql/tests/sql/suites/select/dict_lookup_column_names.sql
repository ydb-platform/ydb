/* syntax version 1 */
/* postgres can not */
use plato;
$dictList = (select AsDict(AsTuple(value, CAST(subkey AS Int32))) as `dict`, subkey, value from Input);
select input.`dict`[input.value], input.`dict`[input.subkey] from $dictList as input;
