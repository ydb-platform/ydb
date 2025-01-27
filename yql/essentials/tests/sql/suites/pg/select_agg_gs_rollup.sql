--!syntax_pg
select x,y
from (
  values (1,2),(1,2),(2,3),(2,2)
) a(x,y)
group by rollup(x,y)
order by x,y 