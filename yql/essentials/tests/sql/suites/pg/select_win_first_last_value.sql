--!syntax_pg
select x,first_value(x) over w,last_value(x) over w
from (values (1),(2),(2),(3)) as a(x)
window w as (order by x)
order by x