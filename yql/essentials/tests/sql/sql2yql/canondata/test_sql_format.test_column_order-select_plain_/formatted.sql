/* postgres can not */
/* syntax version 1 */
USE plato;

PRAGMA OrderedColumns;

SELECT
    *
FROM
    Input
ORDER BY
    subkey
;

SELECT
    *
WITHOUT
    key
FROM
    Input
ORDER BY
    subkey
;

SELECT
    a.*
FROM
    Input AS a
ORDER BY
    a.subkey
;

SELECT
    a.*
WITHOUT
    key
FROM
    Input AS a
ORDER BY
    a.subkey
;

SELECT
    1 AS z,
    2 AS x,
    a.*
FROM
    Input AS a
ORDER BY
    a.subkey
;

SELECT
    1 AS z,
    2 AS x,
    a.*
WITHOUT
    key
FROM
    Input AS a
ORDER BY
    a.subkey
;

SELECT
    1 AS c,
    2 AS b,
    3 AS a
;
