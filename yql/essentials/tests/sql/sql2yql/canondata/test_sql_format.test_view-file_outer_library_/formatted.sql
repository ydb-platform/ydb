/* postgres can not */
/* syntax version 1 */
/* custom error: does not exist*/
PRAGMA yt.ViewIsolation = 'true';

USE plato;

PRAGMA library('mylib.sql');

SELECT
    k,
    s,
    v
FROM
    Input VIEW file_outer_library
;
