insert into plato.Output
select key as x from plato.Input
union all
select value as x from plato.Input;

