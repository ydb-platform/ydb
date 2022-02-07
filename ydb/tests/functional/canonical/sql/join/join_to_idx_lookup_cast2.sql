PRAGMA SimpleColumns;

$input = (SELECT Key, CAST(Fk21 AS Uint8) ?? 0 AS Fk21, Value AS Value1 FROM InputJoin1);

SELECT *
FROM $input AS input
LEFT JOIN InputJoin2 AS t2 ON input.Fk21 = t2.Key1
WHERE Value1 = "Value2"
ORDER BY Value;
