--!syntax_v1

DELETE FROM Input1 ON (Group, Name) VALUES
    (1, "Name1"),
    (1, "Name2");

REPLACE INTO Input1 (Group, Name, Amount) VALUES
    (1, "Name2", 1002),
    (1, "Name3", 1003),
    (1, "Name4", 1004);

UPSERT INTO Input1 (Group, Name, Comment) VALUES
    (1, "Name3", "Upserted"),
    (1, "Name4", "Upserted"),
    (1, "Name5", "Upserted"),
    (1, "Name6", "Upserted");

DELETE FROM Input1 ON (Group, Name) VALUES
    (1, "Name5");
