/* syntax version 1 */
/* postgres can not */
use plato;

--insert into Output
select
  groupTribit,
  count(*) as count
from Input
GROUP BY TableRecordIndex() % 3 as groupTribit
ORDER BY groupTribit, count
