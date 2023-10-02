insert into plato.Output
select a.key as x, b.value as y from plato.Input as a
join plato.Input as b on a.key = b.key
order by x;

