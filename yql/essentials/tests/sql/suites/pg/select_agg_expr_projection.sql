--!syntax_pg
select x+1
from (values (1),(1)) a(x) 
group by x+1
