/* postgres can not */
/* custom error:Anonymous table "@a" must be materialized. Use COMMIT before reading from it*/
USE plato;

SELECT
    *
FROM
    @a
;
