/* syntax version 1 */
/* postgres can not */
use plato;
select *
from Input
group by TableRow().key as k
order by k;
