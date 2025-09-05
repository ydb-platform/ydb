/* postgres can not */
/* syntax version 1 */
/* custom error:Table "Output" cannot have any view after replacing its content*/
insert into plato.Output with truncate
select * from plato.Input;

commit;

select * from plato.Output view raw;
