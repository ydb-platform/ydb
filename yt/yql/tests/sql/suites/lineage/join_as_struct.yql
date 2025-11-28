INSERT INTO plato.Output
SELECT * FROM (
SELECT x,y FROM (
SELECT
    key, <|a:key,b:value|> as x
FROM plato.Input
) as a 
JOIN (
SELECT
    key, <|c:key,d:value|> as y
FROM plato.Input
) as b
ON a.key = b.key
)
flatten columns
