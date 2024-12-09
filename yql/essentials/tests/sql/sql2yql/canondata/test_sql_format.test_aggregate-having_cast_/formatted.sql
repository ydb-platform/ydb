SELECT
    value
FROM plato.Input
GROUP BY
    value
HAVING avg(CAST(key AS int)) > 100
ORDER BY
    value;
