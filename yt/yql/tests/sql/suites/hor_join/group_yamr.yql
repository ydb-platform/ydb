USE plato;

SELECT * FROM (
    SELECT 1 as key, subkey, '' as value from plato.Input1
    UNION ALL
    SELECT 2 as key, '' as subkey, value from plato.Input2
    UNION ALL
    SELECT 3 as key, subkey, '' as value from plato.Input3
    UNION ALL
    SELECT 4 as key, '' as subkey, value from plato.Input4
) AS x
ORDER BY key, subkey, value
;
