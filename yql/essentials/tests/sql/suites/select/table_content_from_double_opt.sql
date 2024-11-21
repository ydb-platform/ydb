/* postgres can not */
/* syntax version 1 */
use plato;

insert into @tmp with truncate
select Just(Just(key)) as key from Input;

commit;

$key = select key from @tmp;
select * from Input where key = $key;
