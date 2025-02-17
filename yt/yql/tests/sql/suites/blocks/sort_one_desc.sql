USE plato;
pragma yt.UseNativeDescSort;

SELECT
    subkey+0 as subkey1, value
FROM Input
ORDER BY subkey1, value desc;
