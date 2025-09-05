INSERT INTO plato.Output
SELECT * FROM (
SELECT
    lead(TableRow()) over ()
FROM plato.Input)
flatten columns
