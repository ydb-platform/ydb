USE plato;

INSERT INTO Output
SELECT
    key AS key,
    "" AS subkey,
    "value:" || value AS value
FROM
    Input
WHERE
    key < "100"
ORDER BY
    key
LIMIT 5;

INSERT INTO Output
SELECT
    *
FROM
    Input
ORDER BY
    key
LIMIT 4;
