--!syntax_pg
select distinct on (x) x,y
from (values (1,1),(1,2),(2,5),(2,4)) a(x,y)

