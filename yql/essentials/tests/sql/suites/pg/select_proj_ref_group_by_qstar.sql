--!syntax_pg
select
a.*
from (values (3),(2),(1),(2)) a(x) 
group by 1

