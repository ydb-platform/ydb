INSERT INTO plato.Output
SELECT * FROM (
SELECT
    TableRow() as x
FROM plato.Input
UNION ALL
SELECT
    1 as y
FROM plato.Input
)
flatten columns
