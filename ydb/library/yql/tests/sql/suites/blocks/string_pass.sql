USE plato;

SELECT
    key, key + subkey, value, value_utf
FROM Input
ORDER BY key
