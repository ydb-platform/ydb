--!syntax_pg
select x
from (values (1,2,3)) a(x,y,z)
group by x,y
order by y,count(z)