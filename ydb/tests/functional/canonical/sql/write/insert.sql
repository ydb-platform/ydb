--!syntax_v1
INSERT INTO Input (key, subkey, value) VALUES
    (3u, 3u, "three"),
    (4u, 4u, "four"),
    (3u, 5u, "five");

INSERT INTO Input1
SELECT Group + 100u AS Group, Name, 17 AS Amount
FROM Input1
WHERE Group = 1;
