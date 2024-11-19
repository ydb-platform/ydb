INSERT INTO plato.Output
SELECT * FROM (
SELECT x FROM (
SELECT
    <|a:key,b:value|> as x
FROM plato.Input
)
group by x
)
flatten columns
