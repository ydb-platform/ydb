/* postgres can not */
/* syntax version 1 */
use plato;

insert into @tmp with truncate
select key from Input order by key desc;

commit;

$key = select key from @tmp;
select * from Input where key = $key;
