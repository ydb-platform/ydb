-- kikimr only: pragma kikimr.UnwrapReadTableValues = "false"; create table plato.Output (key varchar null, subkey varchar null, value varchar null, primary key (key)); commit;
INSERT INTO plato.Output (
    key,
    subkey,
    value
)
SELECT
    key,
    subkey,
    value
FROM
    plato.Input
;

INSERT INTO plato.Output (
    key,
    subkey,
    value
)
SELECT
    key,
    subkey,
    value
FROM
    plato.Input2
;
