/* syntax version 1 */
/* postgres can not */
use plato;

select
  value,
  max(value) over (partition by cast(TableName() as Utf8)),
  cast(TableName()  as Utf8),
from Input order by value;

select
  value,
  max(value) over (order by cast(TableName() as Utf8) rows between unbounded preceding and unbounded following),
  cast(TableName()  as Utf8),
from Input order by value;
