/* postgres can not */
/* custom error:Anonymous table "@a" must be materialized. Use COMMIT before reading from it*/
use plato;

select * from @a;

