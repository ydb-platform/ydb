/* postgres can not */
/* multirun can not */
/* custom error:Modification of "Output" view is not supported*/
INSERT INTO plato.Output
SELECT
    *
FROM plato.Input;
