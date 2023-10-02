USE plato;
SELECT
    key,
    value,
    count(*) AS c
FROM Input
GROUP BY
    key,
    value
ORDER BY c, key, value;
