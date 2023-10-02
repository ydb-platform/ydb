--!syntax_pg
select x,y,count(x) over w 
from (values (1,2),(null::int4,3),(2,4),(2,5)) as a(x,y)
window w as ();

select x,y,count(x) over w 
from (values (1,2),(null::int4,3),(2,4),(2,5)) as a(x,y)
window w as (order by y);
