--!syntax_pg
select 1
from (values (1),(1)) a(x) 
group by x+1
order by x+1
