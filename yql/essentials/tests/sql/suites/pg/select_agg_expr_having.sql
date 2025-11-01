--!syntax_pg
select 1
from (values (1),(1)) a(x) 
group by x+1
having x+1=2
