/* custom error:File not found*/
PRAGMA yt.ViewIsolation = 'true';
USE plato;

SELECT
    k,
    s,
    v
FROM Input
    VIEW file_outer;
