/* postgres can not */
use plato;

select * from Input;

insert into Output with truncate
select cast(-1 as date32), cast(-1 as datetime64), cast(-1 as timestamp64), cast(-1 as interval64);

commit;

select * from Output;
