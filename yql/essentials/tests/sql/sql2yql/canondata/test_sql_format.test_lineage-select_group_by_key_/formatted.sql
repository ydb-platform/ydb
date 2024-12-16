INSERT INTO plato.Output
SELECT
    key,
    count(*),
    min(value)
FROM
    plato.Input
GROUP BY
    key
ORDER BY
    key
;
