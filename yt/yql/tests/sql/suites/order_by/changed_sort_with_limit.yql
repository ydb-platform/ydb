USE plato;

INSERT INTO Output
SELECT
    key,
    '1' as subkey,
    value || "a" as value
FROM Input
WHERE key < "100"
ORDER BY value
limit 3;
