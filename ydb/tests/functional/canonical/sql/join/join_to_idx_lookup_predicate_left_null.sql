

SELECT t1.Fk21 AS Result1, t2.Value AS Result2
FROM InputJoin1 AS t1
JOIN InputJoin2 AS t2 ON t1.Fk21 = t2.Key1
WHERE t1.Value IS NULL OR t1.Value = "Value2"
ORDER BY Result1;
