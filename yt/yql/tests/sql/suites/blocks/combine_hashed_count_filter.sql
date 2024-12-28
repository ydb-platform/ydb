USE plato;

SELECT
    key,
    count(*),
FROM Input
WHERE subkey!=4
GROUP by key
ORDER by key