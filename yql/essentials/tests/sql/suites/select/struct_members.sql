/* postgres can not */
use plato;
$structList = (select AsStruct(key as k, value as v) as `struct` from Input);
select input.`struct`.k as key, input.`struct`.v as value, input.`struct` as `struct` from $structList as input;
