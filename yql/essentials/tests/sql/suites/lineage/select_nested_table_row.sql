insert into plato.Output
select StablePickle(TableRow()) from plato.Input;

