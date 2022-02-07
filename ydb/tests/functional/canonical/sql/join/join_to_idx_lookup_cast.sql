SELECT tt.Value, t2.Value
FROM InputJoin6 AS tt
LEFT JOIN InputJoin2 AS t2
ON tt.Fk21 = t2.Key1 AND tt.Fk22 = t2.Key2
WHERE tt.Value = "Value1"
ORDER BY t2.Value;
