USE plato;

SELECT * FROM (
    SELECT 1 as key, subkey, '' as value from plato.Input1
    UNION ALL
    SELECT 1 as key, '' as subkey, value from plato.Input2
    UNION ALL
    SELECT CAST(key as Int32) as key, '' as subkey, value from plato.Input3
) ORDER BY key, subkey, value
;
