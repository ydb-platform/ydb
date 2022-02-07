

SELECT t1.Value AS Value1, t2.Value AS Value2
FROM InputJoin1 AS t1
LEFT JOIN InputJoin2 AS t2
ON t1.Fk21 == t2.Key1 AND t1.Fk22 == t2.Key2
WHERE t1.Key > 1 AND t1.Key <= 5
ORDER BY Value1, Value2;
