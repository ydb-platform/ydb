USE plato;

pragma yt.EnableFuseMapToMapReduce="true";

SELECT
    key,
    max(subkey),
FROM Input
GROUP BY key;
