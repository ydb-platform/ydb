USE plato;

SELECT
    key,
    min(value),
    max(value),
FROM
    Input
GROUP BY
    key
ORDER BY
    key
;
