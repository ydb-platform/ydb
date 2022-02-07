--!syntax_v1
REPLACE INTO Input1 (Name, Group, Amount) VALUES
    ("Name1", 1, 500),
    ("Name10", 1, 600);

REPLACE INTO Input1 (Name, Group, Comment) VALUES
    ("Name1", 2, "Replaced1"),
    ("Name10", 2, "Replaced10");

REPLACE INTO Input
SELECT key + 1u AS key, subkey, "Replaced" AS value
FROM Input
WHERE key > 1;
