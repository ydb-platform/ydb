--!syntax_v1
SELECT t1.Value AS Value1, t2.Value AS Value2 
FROM InputJoin1 AS t1 
INNER JOIN InputJoinIndex2 VIEW Index AS t2 
ON t1.Fk21 == t2.Fk2
WHERE t1.Key = 3;
