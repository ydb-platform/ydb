SELECT * FROM (
    SELECT key, subkey, '' as value FROM plato.Input
    UNION ALL
    SELECT * from plato.Input
    UNION ALL
    SELECT '' as key, subkey, value from plato.Input
)
ORDER BY key, subkey, value
;