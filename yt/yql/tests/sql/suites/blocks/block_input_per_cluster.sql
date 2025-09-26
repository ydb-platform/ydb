/* postgres can not */
/* yt can not */
USE plato;

PRAGMA plato.JobBlockInput;

SELECT
    key,
    subkey,
    "value: " || value AS value,
FROM plato.PInput
WHERE key < "100"
ORDER BY key;

SELECT
    key,
    subkey,
    "value: " || value AS value,
FROM banach.BInput
WHERE key < "100"
ORDER BY key;
