use plato;

INSERT INTO Output
SELECT
    key as key,
    "" as subkey,
    "value:" || value as value
FROM Input
WHERE key < "100"
ORDER BY key
LIMIT 5;

INSERT INTO Output
SELECT
    *
FROM Input
ORDER BY key
LIMIT 4;
