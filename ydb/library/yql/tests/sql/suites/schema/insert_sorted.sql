/* multirun can not */
insert into plato.Output select * from plato.Input order by d, a, b, c;
commit;
select * from plato.Output;
