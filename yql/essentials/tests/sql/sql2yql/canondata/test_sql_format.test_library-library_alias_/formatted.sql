/* postgres can not */
PRAGMA library('lib2.sql');

IMPORT lib2 SYMBOLS $mul AS $multiply;

SELECT
    $multiply(2, 3)
;
