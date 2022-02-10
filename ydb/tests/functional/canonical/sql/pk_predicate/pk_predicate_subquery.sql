--!syntax_v1

$group = (
    SELECT Type FROM Input3 WHERE Value = "Value_7"
);

SELECT * FROM Input1
WHERE Group = $group;
