--!syntax_pg
select
x
from (values (3),(2),(1),(2)) a(x) 
group by 1

