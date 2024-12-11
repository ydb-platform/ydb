USE plato;

SELECT
    key,
    some(value) AS value
FROM
    Input
WHERE
    key > "999"
GROUP BY
    key
ORDER BY
    key
LIMIT 10;
