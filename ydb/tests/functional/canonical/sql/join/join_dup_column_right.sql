SELECT Key, Key1, Key2
FROM InputJoin1 AS t1
LEFT JOIN InputJoin2 AS t2
ON t1.Key = t2.Key1 AND t1.Fk21 = t2.Key1
WHERE t1.Value == "Value1"
ORDER BY Key;
