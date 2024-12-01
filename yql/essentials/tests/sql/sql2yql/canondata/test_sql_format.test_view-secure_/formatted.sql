/* custom error:SecureParam function can't be used in views*/
PRAGMA yt.ViewIsolation = 'true';
USE plato;

SELECT
    k,
    s,
    v
FROM Input
    VIEW secure;
