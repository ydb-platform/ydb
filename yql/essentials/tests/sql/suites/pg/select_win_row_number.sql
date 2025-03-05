--!syntax_pg
select x,row_number() over w r1,row_number() over () r2
from (values (3),(5)) as a(x)
window w as ()
