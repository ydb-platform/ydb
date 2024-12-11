--!syntax_pg
select x,y,lead(y) over w,lag(y) over w
from (values (1,2),(1,3),(2,4),(2,5)) as a(x,y)
window w as (partition by a.x order by a.y desc)
order by x,y;
