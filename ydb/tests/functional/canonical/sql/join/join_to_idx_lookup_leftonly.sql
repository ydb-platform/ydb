

SELECT t2.Value AS Value2
FROM InputJoin2 AS t2
LEFT ONLY JOIN InputJoin3 AS t3
ON t2.Fk3 == t3.Key
ORDER BY Value2;
