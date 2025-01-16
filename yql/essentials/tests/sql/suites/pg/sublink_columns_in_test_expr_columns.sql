--!syntax_pg
select x = any (select 1)
from (values (1,1),(2,5),(3,4)) a(x,y);

select *
from (values (1,1),(2,5),(3,4)) a(x,y)
where x = all (select 1);

select 1
from (values (1,1),(2,5),(3,4)) a(x,y)
order by x in (select 1);