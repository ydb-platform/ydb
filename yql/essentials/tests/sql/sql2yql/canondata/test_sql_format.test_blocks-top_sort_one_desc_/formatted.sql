USE plato;

PRAGMA yt.UseNativeDescSort;

SELECT
    subkey + 0 AS subkey1,
    value
FROM
    Input
ORDER BY
    subkey1 DESC,
    value
LIMIT 2;
