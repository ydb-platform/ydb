USE plato;

SELECT
    count(*),
    count(key)
FROM Input
WHERE subkey!=5