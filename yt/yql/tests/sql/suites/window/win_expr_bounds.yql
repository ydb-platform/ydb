/* syntax version 1 */
/* postgres can not */
use plato;

declare $begin as Int32;

select
    key, subkey,
    COUNT(*) over w as cnt
from Input4
window
    w as (order by key, subkey rows between $begin preceding and 1 + 1 following)
order by key, subkey;
