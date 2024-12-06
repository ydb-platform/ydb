/* postgres can not */
/* multirun can not */
-- kikimr only: pragma kikimr.UnwrapReadTableValues = "false"; create table plato.Output (key varchar null, subkey varchar null, value varchar null, primary key (key)); commit;
INSERT INTO plato.Output (
    key,
    subkey,
    new_value,
    one_more_value
)
SELECT
    key,
    subkey,
    value AS new_value,
    "x"
FROM
    plato.Input
;
COMMIT;

SELECT
    key,
    subkey,
    new_value,
    one_more_value
FROM
    plato.Output
;
