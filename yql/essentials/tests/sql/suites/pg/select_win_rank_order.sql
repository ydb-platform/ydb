--!syntax_pg
select x,rank() over w,dense_rank() over w 
from (values (1),(2),(2),(3)) as a(x)
window w as (order by x)
