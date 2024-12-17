INSERT INTO plato.Output
SELECT
    *
FROM
    plato.Input
WHERE
    key > ''
;
