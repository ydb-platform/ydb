/* postgres can not */
USE plato;

INSERT INTO Output
SELECT
    *
FROM
    Input
ORDER BY
    key DESC,
    subkey DESC
;
