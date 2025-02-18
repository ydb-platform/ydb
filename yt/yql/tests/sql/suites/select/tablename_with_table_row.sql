USE plato;

SELECT
    TableName() AS tn,
    ROW_NUMBER() OVER () AS rowid,
FROM
    Input
WHERE
    key > "010"
ORDER BY rowid
;
