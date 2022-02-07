SELECT t1.Key AS Key1
FROM InputJoin1 AS t1
LEFT SEMI JOIN InputJoin2 AS t2
ON t1.Fk21 == t2.Key1
WHERE t1.Value != "Value2"
ORDER BY Key1;
