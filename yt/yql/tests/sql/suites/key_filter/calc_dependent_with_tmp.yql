/* postgres can not */
USE plato;

insert into @temp
select * from Input order by key desc limit 1; 

commit;

$last_key = select key from @temp limit 1;

select * from Input where key = $last_key;