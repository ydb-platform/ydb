

SELECT t1.Value, t2.Value
FROM InputJoin1 AS t1
INNER JOIN InputJoin2 AS t2
ON t1.Fk22 == t2.Key2
WHERE t2.Key1 == 101 AND t1.Value == "Value1"
ORDER BY t1.Value, t2.Value;
