USE plato;

SELECT * FROM (
    SELECT key, value from Input
    UNION ALL
    SELECT subkey as key, value from Input2
)
ORDER BY key, value
;
