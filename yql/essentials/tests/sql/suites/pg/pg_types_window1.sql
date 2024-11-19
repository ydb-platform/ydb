--!syntax_pg
select string_agg(x,',') over w
from (values ('a'),('b'),('c')) a(x)
window w as (order by x)
