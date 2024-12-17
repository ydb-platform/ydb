INSERT INTO plato.Output
SELECT
    key AS x
FROM
    plato.Input
UNION ALL
SELECT
    value AS x
FROM
    plato.Input
;
