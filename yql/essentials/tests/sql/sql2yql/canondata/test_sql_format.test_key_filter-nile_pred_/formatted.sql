SELECT
    *
FROM plato.Input
WHERE (key > "023") IS NOT NULL AND Unwrap(key > "023")
ORDER BY
    key,
    subkey;
