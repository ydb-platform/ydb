insert into plato.Output
select count(*), min(value) from plato.Input;

