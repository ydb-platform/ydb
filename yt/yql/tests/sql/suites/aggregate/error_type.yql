/* custom error: Uncompatible member x types: Int32 and String */
use plato;

insert into @foo
select 1 as x;

insert into @bar
select 'z' as x;

commit;

select * from @foo
union
select * from @bar;


