USE plato;
PRAGMA library('lib1.sql');

IMPORT lib1 symbols $subq;

SELECT * FROM $subq();
