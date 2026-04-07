--!syntax_pg
select x,row_number() over w,lag(x) over w,lead(x) over w
from (values ('a'),('b'),('c')) a(x)
window w as (order by x)
