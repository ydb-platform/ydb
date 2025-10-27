/* postgres can not */
USE plato;

$visitors = (
SELECT
    key,
    subkey,
    value
FROM Input
WHERE subkey != ""
);

$over_threshold = (
SELECT
    key,
    subkey,
    value
FROM $visitors
WHERE key > "070"
);

$clean = (SELECT COUNT(*) FROM $over_threshold);

$tail = (
SELECT
    key,
    subkey,
    value
FROM $visitors
ORDER BY key DESC
LIMIT IF($clean ?? 0 < 2, 2 - $clean ?? 0, 0)
);

INSERT INTO Output WITH TRUNCATE
SELECT
    key,
    subkey,
    value
FROM $over_threshold
UNION ALL
SELECT
    key,
    subkey,
    value
FROM $tail;
