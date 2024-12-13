INSERT INTO plato.Output
SELECT
    count(*),
    min(value)
FROM
    plato.Input
;
