--!syntax_pg
select count(x) as y
from (values (1)) a(x)
order by sum(x)
