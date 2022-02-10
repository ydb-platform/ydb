

SELECT t1.Fk21 AS Key, COUNT(t2.Key) AS Cnt
FROM InputJoin1 AS t1
LEFT JOIN InputJoin1 AS t2
ON t1.Fk21 == t2.Key
WHERE t1.Value == "Value1"
GROUP BY t1.Fk21
ORDER BY Key;
