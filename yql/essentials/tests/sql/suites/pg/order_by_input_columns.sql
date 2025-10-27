--!syntax_pg
select y
from (values (1,1),(2,5),(7,4)) a(x,y)
order by x desc