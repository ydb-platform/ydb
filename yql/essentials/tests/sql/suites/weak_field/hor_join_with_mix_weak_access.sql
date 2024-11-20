/* postgres can not */
/* syntax version 1 */
use plato;

select key, WeakField(value1, "String", "funny") as value from Input
union all
select key, _other["value1"] as value from Input;
