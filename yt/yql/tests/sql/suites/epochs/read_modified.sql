/* postgres can not */
/* multirun can not */
use plato;

insert into Output select * from Output;
insert into Output select * from Output;

commit;

select count(*) from Output;

insert into Output select * from Output;
insert into Output select * from Output;

commit;

select count(*) from Output;
