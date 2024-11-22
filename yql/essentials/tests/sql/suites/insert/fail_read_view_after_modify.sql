/* postgres can not */
/* syntax version 1 */
insert into plato.Output with truncate
select * from plato.Input;

commit;

select * from plato.Output view raw;
