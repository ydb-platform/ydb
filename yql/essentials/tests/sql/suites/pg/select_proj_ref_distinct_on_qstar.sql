--!syntax_pg
select distinct on (1)
a.*
from (values (3),(2),(1),(2)) a(x) 
