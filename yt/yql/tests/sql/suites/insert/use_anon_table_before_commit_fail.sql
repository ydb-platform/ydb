/* postgres can not */
/* custom error:Anonymous table "@a" must be materialized. Use COMMIT before reading from it*/
use plato;

insert into @a
select * from Input;

select * from @a;

