SELECT
    key,
    subkey
FROM plato.Input
WHERE NOT value OR Random(key) >= 0.0
ORDER BY
    key;
