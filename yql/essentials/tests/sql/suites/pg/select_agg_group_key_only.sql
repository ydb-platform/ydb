--!syntax_pg
select x
from (values (1,2),(3,4),(3,5)) u(x,y)
group by x

