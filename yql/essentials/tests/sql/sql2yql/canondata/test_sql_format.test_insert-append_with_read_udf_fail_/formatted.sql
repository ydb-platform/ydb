/* postgres can not */
/* multirun can not */
/* custom error:Table "Output" has udf remappers, append is not allowed*/
INSERT INTO plato.Output
SELECT
    *
FROM plato.Input;
