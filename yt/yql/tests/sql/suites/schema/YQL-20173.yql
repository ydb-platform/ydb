USE plato;

PRAGMA yt.JobBlockInput;

SELECT
    key,
    subkey,
    "value: " || value AS value,
FROM Input
WHERE key < "100"
ORDER BY key;
