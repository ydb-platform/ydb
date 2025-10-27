insert into plato.Output
select value from
(
    select key as x, value from plato.Input
    union all
    select AsList(key) as x, value from plato.Input
);
