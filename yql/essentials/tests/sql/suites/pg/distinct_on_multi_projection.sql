--!syntax_pg
select 10 as u,20 as v
union all
select distinct on (x) x,y
from (values (1,1),(1,2),(2,5),(2,4)) a(x,y)
order by u,v desc
