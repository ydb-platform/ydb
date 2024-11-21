USE plato;

SELECT
    key, key + subkey, value, value_utf
FROM Input
WHERE subkey != 4
ORDER BY key
