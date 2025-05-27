/* multirun can not */
insert into plato.Output select * from plato.Input where a is not null;
commit;
select * from plato.Output;
