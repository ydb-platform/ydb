/* postgres can not */
use plato;

insert into @foo
select null as x, 1 as y;

commit;

insert into @foo
select 2 as y;

commit;

select * from @foo;
