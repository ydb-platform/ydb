--!syntax_pg
select -x as x
from (values (1,1),(2,5),(3,4)) a(x,y)
order by (select x)