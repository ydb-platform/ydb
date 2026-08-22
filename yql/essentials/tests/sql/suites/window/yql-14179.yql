select 
  x,
  aggregate_list(x) over w as lst,
from (values (1),(2),(3)) as a(x)
window
  w as (rows between 0 preceding and 0 preceding)
order by x;
