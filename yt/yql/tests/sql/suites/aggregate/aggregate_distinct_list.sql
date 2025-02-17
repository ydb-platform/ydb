/* syntax version 1 */
/* postgres can not */
use plato;

insert into @foo
select AsList(1,2) as x 
union all
select AsList(1,3) as x 
union all
select AsList(1,2) as x;

commit;

select listlength(aggregate_list(distinct x)) as c 
from @foo;

select count(distinct x) as c 
from @foo;

insert into @bar
select AsList(1,2) as x,AsList(4) as y 
union all
select AsList(1,3) as x,AsList(4) as y 
union all
select AsList(1,3) as x,AsList(4) as y 
union all
select AsList(1,3) as x,AsList(4) as y 
union all
select AsList(1,2) as x,AsList(5) as y 
union all
select AsList(1,2) as x,AsList(5) as y;

commit;

select x,count(distinct y) as c
from @bar 
group by x
order by c;
