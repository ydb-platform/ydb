PRAGMA library('lib1.yqls');

IMPORT lib1 SYMBOLS $sqr;

SELECT
    $sqr(10)
;
