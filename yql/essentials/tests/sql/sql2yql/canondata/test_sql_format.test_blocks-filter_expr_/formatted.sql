SELECT
    key,
    subkey
FROM plato.Input
WHERE NOT value
ORDER BY
    key;
