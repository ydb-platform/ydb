SELECT
    value,
    count(DISTINCT key) AS dist,
    count(key) AS full
FROM plato.Input2
GROUP BY
    value
ORDER BY
    value;
