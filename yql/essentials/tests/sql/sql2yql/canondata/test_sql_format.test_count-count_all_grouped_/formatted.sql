SELECT
    key,
    count(*)
FROM plato.Input
GROUP BY
    key
ORDER BY
    key;
