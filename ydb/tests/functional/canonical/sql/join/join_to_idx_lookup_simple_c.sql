PRAGMA SimpleColumns;

$input = (SELECT Fk21 ?? 0 AS Key1, Fk22 AS Key2, Value ?? "Empty" AS ValueInput FROM InputJoin1);

SELECT *
FROM $input AS input
LEFT JOIN InputJoin2 AS t2 ON input.Key1 = t2.Key1 AND input.Key2 = t2.Key2
WHERE input.ValueInput != "Value1"
ORDER BY Key1, Key2;
