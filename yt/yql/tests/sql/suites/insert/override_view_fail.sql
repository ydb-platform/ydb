/* postgres can not */
/* custom error:Modification of "Output" view is not supported*/
insert into plato.Output with truncate
select * from plato.Input;

