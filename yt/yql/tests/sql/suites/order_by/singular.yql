/* postgres can not */
/* syntax version 1 */
/* hybridfile can not YQL-17743 */
use plato;

insert into @foo
select void() as x,null as y,[] as z,{} as w
order by x,y,z,w;

commit;

select * from @foo;
