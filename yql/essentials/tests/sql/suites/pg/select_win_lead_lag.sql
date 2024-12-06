--!syntax_pg
select x,lead(x) over w,lag(x) over w
from (values (3),(4),(5)) as a(x)
window w as ()
