

SELECT t1.Fk21 AS Result1, t2.Value AS Result2
FROM InputJoin1 AS t1
JOIN InputJoin2 AS t2 ON t1.Fk21 = t2.Key1 AND t1.Fk22 = t2.Key2
WHERE t2.Fk3 IN ("Name1", "Name2") AND t2.Value > "Value26"
ORDER BY Result1;
