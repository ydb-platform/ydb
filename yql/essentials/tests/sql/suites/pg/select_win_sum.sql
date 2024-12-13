--!syntax_pg
select x,y,sum(x) over w 
from (values (1,2),(1,3),(2,4),(2,5)) as a(x,y)
window w as ();

select x,y,sum(x) over w 
from (values (1,2),(1,3),(2,4),(2,5)) as a(x,y)
window w as (order by y);
