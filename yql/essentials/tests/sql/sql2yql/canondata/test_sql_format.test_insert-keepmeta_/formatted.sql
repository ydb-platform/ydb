/* postgres can not */
/* multirun can not */
INSERT INTO plato.Output WITH (
    truncate,
    keepmeta
)
SELECT
    *
FROM
    plato.Input
;
