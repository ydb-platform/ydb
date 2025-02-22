USE plato;
pragma yt.UseNativeDescSort;

SELECT
    key, subkey+0 as subkey1, value
FROM Input
ORDER BY key asc, subkey1 desc, value;
