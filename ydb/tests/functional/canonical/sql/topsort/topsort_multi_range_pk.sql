$fetch = (
    SELECT * FROM Input1 WHERE (
        Group > 10
        OR
        Group = 10 AND Name > "Name4")
);

SELECT * FROM $fetch WHERE Amount != 100
ORDER BY Group, Name LIMIT 3;
