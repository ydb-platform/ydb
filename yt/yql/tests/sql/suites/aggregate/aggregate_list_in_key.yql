/* syntax version 1 */
/* postgres can not */
use plato;
insert into @foo
select AsList(1,2) as x,1 as y 
union all
select AsList(1,3) as x,2 as y 
union all
select AsList(1,2) as x,3 as y;
commit;

select x,count(*) as c
from @foo
group by x
order by c;

insert into @bar
select AsList(1,2) as x,AsList(4) as y, 1 as z 
union all
select AsList(1,3) as x,AsList(4) as y, 2 as z
union all
select AsList(1,3) as x,AsList(4) as y, 3 as z 
union all
select AsList(1,3) as x,AsList(4) as y, 4 as z 
union all
select AsList(1,2) as x,AsList(5) as y, 5 as z 
union all
select AsList(1,2) as x,AsList(5) as y, 6 as z;
commit;

select x,y,count(*) as c 
from @bar 
group by x, y
order by c;

select x,y,count(distinct z) as c 
from @bar 
group by x,y
order by c;

select x,y, min(z) as m, count(distinct z) as c 
from @bar
group by x,y
order by c;

select x
from @bar as t 
group by x 
order by t.x[1];

select x,y
from @bar as t
group by x, y 
order by t.x[1],t.y[0];

select distinct x
from @bar as t
order by t.x[1] desc;

select distinct x,y 
from @bar as t 
order by t.x[1] desc,t.y[0] desc;
