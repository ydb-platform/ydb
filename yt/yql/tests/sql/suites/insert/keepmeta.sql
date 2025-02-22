/* postgres can not */
/* multirun can not */
insert into plato.Output with (truncate, keepmeta)
select * from plato.Input;

