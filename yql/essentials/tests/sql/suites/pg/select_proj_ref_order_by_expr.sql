--!syntax_pg
select
x+1
from (values (3),(2),(1),(2)) a(x) 
order by 1

