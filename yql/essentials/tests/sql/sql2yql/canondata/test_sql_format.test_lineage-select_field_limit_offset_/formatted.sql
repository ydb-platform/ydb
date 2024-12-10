INSERT INTO plato.Output
SELECT
    key
FROM
    plato.Input
ORDER BY
    key
LIMIT 4 OFFSET 1;
