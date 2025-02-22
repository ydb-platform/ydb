/* syntax version 1 */
/* postgres can not */
use plato;

$input = select a.*, [1,2] as lst from Input as a;

select key, subkey, some(lst) as lst_count
from $input flatten list by lst
where lst != 1
group by grouping sets ((key), (key, subkey))
order by key, subkey;

