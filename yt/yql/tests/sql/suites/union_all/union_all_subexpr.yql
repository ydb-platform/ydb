SELECT *
FROM (
    SELECT key, CAST(subkey AS int) as subkey, NULL as value FROM plato.Input
    UNION ALL
    SELECT key, NULL as subkey, value from plato.Input
) as x
ORDER BY key, subkey, value;