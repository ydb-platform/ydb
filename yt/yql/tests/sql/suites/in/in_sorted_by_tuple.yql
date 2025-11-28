/* postgres can not */
/* syntax version 1 */
use plato;

select value, AGG_LIST_DISTINCT(tpl) as tuples
from (
    select AsTuple(key, subkey, value) as tpl, value from InputSorted
)
where value in (select DISTINCT value from Input)
group by value;
