$name = (
    SELECT Value2 FROM Input2 WHERE Key = 101
);

SELECT * FROM Input1
WHERE Group = 4 AND Name = $name;
