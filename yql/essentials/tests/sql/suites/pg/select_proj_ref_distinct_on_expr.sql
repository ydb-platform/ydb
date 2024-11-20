--!syntax_pg
select distinct on (1)
x+1
from (values (3),(2),(1),(2)) a(x) 
