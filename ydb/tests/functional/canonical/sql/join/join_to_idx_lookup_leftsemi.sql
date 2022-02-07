

SELECT t1.Value AS Value1
FROM InputJoin1 AS t1
LEFT SEMI JOIN InputJoin2 AS t2
ON t1.Fk21 == t2.Key1 AND t1.Fk22 == t2.Key2
WHERE t1.Value != "Value3"
ORDER BY Value1;
