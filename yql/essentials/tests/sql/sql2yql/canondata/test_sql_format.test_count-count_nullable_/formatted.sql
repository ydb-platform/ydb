SELECT
    count(CAST(key AS int)),
    value
FROM plato.Input
GROUP BY
    value
ORDER BY
    value;
