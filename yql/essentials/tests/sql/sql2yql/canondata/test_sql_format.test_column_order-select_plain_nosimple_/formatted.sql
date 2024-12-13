/* postgres can not */
/* syntax version 1 */
USE plato;

PRAGMA OrderedColumns;
PRAGMA DisableSimpleColumns;

SELECT
    *
FROM
    Input
;

SELECT
    *
WITHOUT
    key
FROM
    Input
;

SELECT
    a.*
FROM
    Input AS a
;

SELECT
    a.*
WITHOUT
    key
FROM
    Input AS a
;

SELECT
    1 AS z,
    2 AS x,
    a.*
FROM
    Input AS a
;

SELECT
    1 AS z,
    2 AS x,
    a.*
WITHOUT
    key
FROM
    Input AS a
;

SELECT
    1 AS c,
    2 AS b,
    3 AS a
;
