USE plato;

PRAGMA library('lib1.sql');

IMPORT lib1 SYMBOLS $subq;

SELECT
    *
FROM
    $subq()
;
