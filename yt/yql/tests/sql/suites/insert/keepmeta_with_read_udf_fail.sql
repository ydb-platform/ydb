/* postgres can not */
/* multirun can not */
/* custom error:Table "Output" has udf remappers, truncate with keep meta is not allowed*/
insert into plato.Output with (truncate, keepmeta)
select * from plato.Input;

