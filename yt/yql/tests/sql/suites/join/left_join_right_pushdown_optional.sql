PRAGMA FilterPushdownOverJoinOptionalSide;

use plato;

SELECT a.Key AS Key, a.Value AS Value, b.y AS RightValue
FROM Input2 AS a
LEFT JOIN (
    SELECT Fk1 AS x, Value AS y
    FROM Input1
) AS b
ON a.Key == b.x
WHERE b.x >= "Name2";
