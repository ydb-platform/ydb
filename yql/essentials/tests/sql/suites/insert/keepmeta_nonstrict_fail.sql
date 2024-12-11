/* postgres can not */
/* multirun can not */
/* custom error:Table "Output" has non-strict schema, truncate with keep meta is not allowed*/
insert into plato.Output with (truncate, keepmeta)
select * from plato.Input;

