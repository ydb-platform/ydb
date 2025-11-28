/* postgres can not */
use plato;

INSERT INTO Output1
SELECT
    key as key,
    "" as subkey,
    "value:" || value as value
FROM Input
WHERE key < "100"
ORDER BY key;

INSERT INTO Output2
SELECT
    key as key,
    "" as subkey,
    "value:" || value as value
FROM Input
WHERE key < "200"
ORDER BY key;

INSERT INTO Output1
SELECT
    *
FROM Input
ORDER BY key;

INSERT INTO Output2
SELECT
    *
FROM Input
ORDER BY key;