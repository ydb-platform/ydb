--!syntax_pg
select 
x,
sum(x) over (order by x rows between unbounded preceding and 1 preceding) as upp,
sum(x) over (order by x rows between unbounded preceding and current row) as upc,
sum(x) over (order by x rows between unbounded preceding and 1 following) as upf,
sum(x) over (order by x rows between unbounded preceding and unbounded following) as upuf,
sum(x) over (order by x rows between 2 preceding and 1 preceding) as pp,
sum(x) over (order by x rows between 2 preceding and current row) as pc,
sum(x) over (order by x rows between 2 preceding and 1 following) as pf,
sum(x) over (order by x rows between 2 preceding and unbounded following) as puf,
sum(x) over (order by x rows between current row and current row) as cc,
sum(x) over (order by x rows between current row and 1 following) as cf,
sum(x) over (order by x rows between current row and unbounded following) as cuf,
sum(x) over (order by x rows between 1 following and 2 following) as ff,
sum(x) over (order by x rows between 1 following and unbounded following) as fuf
from (values (1),(2),(3),(4),(5),(6),(7)) a(x)

