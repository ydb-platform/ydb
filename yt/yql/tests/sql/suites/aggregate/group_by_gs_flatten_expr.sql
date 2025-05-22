/* syntax version 1 */
/* postgres can not */
use plato;

$input = select a.*, [1,2] as lst from Input as a;

select key, subkey, count(lst) as lst_count
from $input flatten list by (ListExtend(lst, [3,4]) as lst)
where lst != 2
group by grouping sets ((key), (key, subkey))
order by key, subkey;

