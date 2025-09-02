/* postgres can not */
/* multirun can not */
/* custom error:Table "Output" has udf remappers, append is not allowed*/
insert into plato.Output
select * from plato.Input;

