/* syntax version 1 */
/* postgres can not */
select
  min(x), 
  count(x), 
  count(*), 
  aggregate_list_distinct(x), 
  aggregate_list(x),
  bool_and(x)
from (
   select null as x union all select Null as x
);

select
  min(x), 
  count(x), 
  count(*), 
  aggregate_list_distinct(x), 
  aggregate_list(x),
  bool_and(x)
from (
   select null as x, 1 as y union all select Null as x, 2 as y
)
group by y;
