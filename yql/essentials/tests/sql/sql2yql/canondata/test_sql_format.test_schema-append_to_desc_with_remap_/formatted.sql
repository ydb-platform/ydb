/* postgres can not */
USE plato;

INSERT INTO Output
SELECT
    *
FROM
    Input
WHERE
    key > '000'
ORDER BY
    key DESC,
    subkey DESC
;
