/* postgres can not */
USE plato;

insert into @tmp
select * from (
    select <||> as a
) flatten columns;

commit;

select 1 as a from @tmp;
