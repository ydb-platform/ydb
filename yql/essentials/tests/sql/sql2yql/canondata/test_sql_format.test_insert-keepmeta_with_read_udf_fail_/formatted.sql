/* postgres can not */
/* multirun can not */
/* custom error:Table "Output" has udf remappers, truncate with keep meta is not allowed*/
INSERT INTO plato.Output WITH (
    truncate,
    keepmeta
)
SELECT
    *
FROM
    plato.Input
;
