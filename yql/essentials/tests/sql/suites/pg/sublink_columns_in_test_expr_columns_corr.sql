--!syntax_pg
select x = any (select x)
from (values (1,1),(2,5),(3,4)) a(x,y);

select *
from (values (1,1),(2,5),(3,4)) a(x,y)
where x = all (select x);

select 1
from (values (1,1),(2,5),(3,4)) a(x,y)
order by x in (select x);