PRAGMA FilterPushdownOverJoinOptionalSide;

SELECT t1.Key1, t1.Key2, t1.Fk1, t1.Value, t2.Key, t2.Value FROM plato.Input1 AS t1
LEFT JOIN plato.Input2 AS t2
ON t1.Fk1 = t2.Key
WHERE t2.Value IS NULL
ORDER BY t1.Key1, t1.Key2;
