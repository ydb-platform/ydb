--!syntax_pg
select distinct on (x/3) x,y
from (values (1,1),(1,2),(2,5),(2,4)) a(x,y)
order by x/3,y desc
