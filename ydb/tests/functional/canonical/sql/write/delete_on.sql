--!syntax_v1

DELETE FROM Input1 ON (Group, Name) VALUES
    (1, "Name1"),
    (1, "Name2"),
    (1, "Name5");

DELETE FROM Input ON
SELECT key, subkey FROM Input
WHERE value = "b";
