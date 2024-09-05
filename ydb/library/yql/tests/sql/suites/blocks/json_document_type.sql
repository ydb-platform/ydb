USE plato;

SELECT
    key,
    SOME(val) as someVal,
FROM Input
GROUP BY key
ORDER BY key
