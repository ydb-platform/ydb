INSERT INTO plato.Output
SELECT * FROM (
SELECT
    lead(<|a:key,b:value|>) over ()
FROM plato.Input)
flatten columns
