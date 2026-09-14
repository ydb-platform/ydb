SELECT * FROM (
    SELECT CAST(key AS int) as key, '' as value FROM plato.Input
    UNION ALL
    SELECT 0 as key, value from plato.Input
)
ORDER BY key, value
;