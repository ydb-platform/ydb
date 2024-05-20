PRAGMA FilterPushdownOverJoinOptionalSide;

SELECT t1.Key1, t1.Key2, t1.Fk1, t1.Value, t2.Key, t2.Value, t3.Value

FROM plato.Input1 AS t1
LEFT JOIN plato.Input2 AS t2
ON t1.Fk1 = t2.Key
INNER JOIN plato.Input3 as t3
ON t1.Fk1 = t3.Key

WHERE t2.Value > 1001;
