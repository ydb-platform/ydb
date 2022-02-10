--!syntax_v1

UPDATE Input ON (key, subkey, value) VALUES
    (1, 2, "a_updated"),
    (1, 3, "non_existent1"),
    (5, 6, "c_updated"),
    (6, 1, "non_existent2");

UPDATE Input1 ON
SELECT Group, Name, Amount - 50 AS Amount
FROM Input1
WHERE Group = 1;
