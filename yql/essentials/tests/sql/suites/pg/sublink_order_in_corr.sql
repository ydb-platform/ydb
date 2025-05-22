--!syntax_pg
select x,y
from (values (1,1),(2,5),(3,4)) a(x,y)
order by 2 not in (select x),y