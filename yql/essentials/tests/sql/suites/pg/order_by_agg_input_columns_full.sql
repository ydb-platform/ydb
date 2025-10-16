--!syntax_pg
select sum(x)
from (values (1,1),(2,5),(2,4)) a(x,y)
order by count(*)
