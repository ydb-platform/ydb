USE plato;

SELECT * FROM (
    SELECT key, value FROM plato.Input1 where key > "010"
    UNION ALL
    SELECT key, value FROM plato.Input2 where key > "020"
) AS x ORDER BY key, value
;
