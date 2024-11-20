SELECT * FROM (
    SELECT CAST(key AS int) as key, '' as subkey, '' as value FROM plato.Input
    UNION ALL
    SELECT 1 as key, subkey, '' as value from plato.Input
    UNION ALL
    SELECT 1 as key, '' as subkey, value from plato.Input
)
ORDER BY key, subkey, value
;