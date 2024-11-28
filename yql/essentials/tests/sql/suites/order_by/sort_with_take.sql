/* postgres can not */
USE plato;

insert into Output with truncate
select * from (select * from Input limit 3)
order by key;
