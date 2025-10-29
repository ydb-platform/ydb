/* syntax version 1 */
/* postgres can not */
select
  min(x) over w, 
  count(x) over w, 
  count(*) over w, 
  aggregate_list_distinct(x) over w, 
  aggregate_list(x) over w,
  bool_and(x) over w
from (
   select null as x union all select Null as x
)
window w as (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);

select
  min(x) over w, 
  count(x) over w, 
  count(*) over w, 
  aggregate_list_distinct(x) over w, 
  aggregate_list(x) over w,
  bool_and(x) over w
from (
   select null as x union all select Null as x
)
window w as (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING);
