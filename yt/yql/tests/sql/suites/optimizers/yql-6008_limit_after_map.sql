/* postgres can not */
/* multirun can not */
USE plato;

insert into Output
select * from plato.Input where value != "111" limit 3;

commit;

insert into Output
select * from plato.Input order by value;
