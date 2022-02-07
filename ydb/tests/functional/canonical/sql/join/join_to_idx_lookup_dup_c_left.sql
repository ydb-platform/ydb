SELECT t1.Value, tt.Value
FROM InputJoin1 AS t1
INNER JOIN InputJoin5 AS tt
ON t1.Fk21 = tt.Key1 AND t1.Fk21 = tt.Key2
WHERE t1.Value == "Value1"
ORDER BY tt.Value;
