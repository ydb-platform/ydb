/* postgres can not */
USE plato;

INSERT INTO Output
SELECT
    "1" AS key,
    "1" AS subkey,
    "1" AS value
;

PRAGMA File("file", "dummy");

INSERT INTO Output
SELECT
    *
FROM
    Input
WHERE
    key < "030"
;
