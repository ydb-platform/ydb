/* syntax version 1 */
select
  key,
  max(key) over (order by key) as running_max,
  lead(key) over (order by key rows unbounded preceding) as next_key,
  aggregate_list(key) over w as keys,
from plato.Input
window w as (order by key rows between unbounded preceding and current row)
order by key;
