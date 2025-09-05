PRAGMA FilterPushdownOverJoinOptionalSide;

SELECT t1.Key1, t1.Key2, t1.Fk1, t1.Value, t2.Key, t2.Value FROM plato.Input2 AS t2
RIGHT JOIN plato.Input1 AS t1
ON t2.Key = t1.Fk1
WHERE t1.Key1 > 104
ORDER BY t1.Key1, t1.Key2;
