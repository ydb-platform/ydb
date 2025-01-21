/* postgres can not */
USE plato;

SELECT
    subkey
FROM Input
WHERE subkey < "100";

SELECT
    value
FROM Input
LIMIT 3;

SELECT
    key
FROM
    (SELECT * FROM Input ORDER BY -CAST(subkey as Int32) LIMIT 5)
;
