/* syntax version 1 */
/* postgres can not */
use plato;

$withStruct = select subkey, value, AsStruct(key as key) as s from Input3;

select count(distinct s.key) as cnt from $withStruct;
