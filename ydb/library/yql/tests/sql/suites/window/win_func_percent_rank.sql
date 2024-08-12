select
r,x,
percent_rank() over w,
percent_rank(x) over w,
from (select * from (values (1,null),(2,3),(3,4),(4,4)) as a(r,x)) as z
window w as (order by r)
order by r

