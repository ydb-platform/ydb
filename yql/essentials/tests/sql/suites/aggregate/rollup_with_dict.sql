/* syntax version 1 */
/* postgres can not */
use plato;

--insert into Output
select val, count(*) as cnt, grouping(val) as grouping
from Input as t
group by rollup(t.`dict`["c"] as val)
order by val, cnt
;
