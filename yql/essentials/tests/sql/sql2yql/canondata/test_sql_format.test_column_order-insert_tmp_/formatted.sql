/* postgres can not */
/* syntax version 1 */
USE plato;
PRAGMA OrderedColumns;

INSERT INTO @tmp
SELECT
    *
FROM
    Input
ORDER BY
    subkey,
    key
;
COMMIT;

SELECT
    *
FROM
    @tmp
ORDER BY
    subkey,
    key
;

INSERT INTO @tmp WITH truncate
SELECT
    key,
    value,
    subkey
FROM
    Input
ORDER BY
    subkey,
    key
;

SELECT
    *
FROM
    @tmp
ORDER BY
    subkey,
    key
;
COMMIT;

SELECT
    *
FROM
    @tmp
ORDER BY
    subkey,
    key
;
