/* postgres can not */
/* syntax version 1 */
pragma yt.ViewIsolation = 'true';
USE plato;
PRAGMA library('mylib.sql');
SELECT k, s, v FROM Input VIEW file_outer_library;
