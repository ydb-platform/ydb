SELECT
    value,
    count(DISTINCT key) AS count
FROM plato.Input2
GROUP BY
    value
ORDER BY
    value;
