/* postgres can not */
/* multirun can not */
/* custom error:Modification of "Output" view is not supported*/
INSERT INTO plato.Output WITH (
    truncate,
    keepmeta
)
SELECT
    *
FROM plato.Input;
