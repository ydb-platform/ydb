SELECT
    *
FROM plato.Input
WHERE
    "1" IN (key, subkey, value) OR
    value NOT IN (key, subkey, value)
;
