/* postgres can not */

$aggregated = (
    SELECT Group, Name, SUM(Amount) AS TotalAmount
    FROM plato.Input1
    GROUP BY Group, Name
);

SELECT t.Comment, a.TotalAmount
FROM plato.Input1 AS t
INNER JOIN $aggregated AS a
ON t.Group == a.Group AND t.Name == a.Name
ORDER BY t.Comment, a.TotalAmount;

SELECT TotalAmount FROM $aggregated ORDER BY TotalAmount;
