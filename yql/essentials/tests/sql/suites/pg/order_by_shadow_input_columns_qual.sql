--!syntax_pg
select -x as x
from (values (1,1),(2,5),(7,4)) a(x,y)
order by a.x desc