--!syntax_v1
UPSERT INTO Input1 (Name, Group, Amount) VALUES
    ("Name1", 1, 500),
    ("Name10", 1, 600);

UPSERT INTO Input1 (Name, Group, Comment) VALUES
    ("Name1", 2, "Upserted1"),
    ("Name10", 2, "Upserted10");

UPSERT INTO Input
SELECT key, subkey + 1u AS subkey, "Upserted" AS value
FROM Input
WHERE key < 5;
