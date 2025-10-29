select
r,x,
ntile(3) over w,
from (select * from (values (1,3),(2,null),(3,4),(4,5)) as a(r,x)) as z
window w as (order by r)
order by r

