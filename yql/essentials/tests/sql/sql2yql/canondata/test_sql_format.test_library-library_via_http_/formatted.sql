/* postgres can not */
/* kikimr can not - range not supported */
PRAGMA library('lib1.sql');
PRAGMA library('lib2.sql');

IMPORT lib1 SYMBOLS $sqr;

SELECT
    $sqr(10)
;
