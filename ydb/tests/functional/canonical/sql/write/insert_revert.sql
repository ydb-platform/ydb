--!syntax_v1
INSERT OR REVERT INTO Input (key, subkey, value) VALUES
    (3u, 3u, "three"),
    (3u, 4u, "four"),
    (3u, 5u, "five");

INSERT OR REVERT INTO Input1 (Group, Name, Comment) VALUES
    (100u, "N1", "Inserted"),
    (100u, "N2", "Inserted"),
    (100u, "N3", "Inserted");
