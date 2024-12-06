/* postgres can not */
/* syntax version 1 */
USE plato;

PRAGMA OrderedColumns;

INSERT INTO Input
SELECT
    key,
    subkey,
    value
FROM
    Input
;

COMMIT;

SELECT
    *
FROM
    Input
ORDER BY
    subkey,
    key
;

INSERT INTO Output
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
    Output
ORDER BY
    subkey,
    key
;

INSERT INTO Output WITH truncate
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
    Output
ORDER BY
    subkey,
    key
;

COMMIT;

SELECT
    *
FROM
    Output
ORDER BY
    subkey,
    key
;
