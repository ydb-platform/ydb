--!syntax_pg
select x, count(*)
from (values (1,1),(3,5),(3,4)) a(x,y)
group by x
order by count(*)+1 desc,x
