-- kikimr only: pragma kikimr.UnwrapReadTableValues = "false"; create table plato.Output (key varchar null, subkey varchar null, value varchar null, primary key (key)); commit;
INSERT INTO plato.Output (
    key,
    subkey,
    value
)
SELECT
    key,
    subkey,
    max(value)
FROM plato.Input
GROUP BY
    key,
    subkey
ORDER BY
    key;
