/* postgres can not */
USE plato;
PRAGMA yt.UseNativeDescSort;

INSERT INTO Output
SELECT
    *
FROM Input
ORDER BY
    key || subkey DESC,
    key DESC;
