--!syntax_v1

DELETE FROM Input1 WHERE Name = "Name1";

DELETE FROM Input1 ON (Group, Name) VALUES
    (1, "Name1"),
    (1, "Name2");
