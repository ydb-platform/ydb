/* postgres can not */
/* multirun can not */
/* custom error:Modification of "Output" view is not supported*/
insert into plato.Output with (truncate, keepmeta)
select * from plato.Input;

