/* postgres can not */
/* syntax version 1 */
/* custom error:Table "Output" cannot have any view after replacing its content*/
INSERT INTO plato.Output WITH truncate
SELECT
    *
FROM plato.Input;
COMMIT;

SELECT
    *
FROM plato.Output
    VIEW raw;
