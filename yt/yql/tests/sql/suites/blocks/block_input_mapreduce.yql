USE plato;

PRAGMA yt.EnableFuseMapToMapReduce;
PRAGMA yt.JobBlockInput;

SELECT
    key
FROM Input
WHERE key < "100"
GROUP BY key;
