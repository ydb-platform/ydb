/* postgres can not */
use plato;

insert into @a
select * from Input;

select * from @a;

