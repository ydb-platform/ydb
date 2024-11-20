--!syntax_pg
select 1
from (values (1,1),(2,5),(2,4)) a(x,y)
group by x
order by x
