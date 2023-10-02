--!syntax_pg
select x,row_number() over w,row_number() over ()
from (values (3),(5)) as a(x)
window w as ()
