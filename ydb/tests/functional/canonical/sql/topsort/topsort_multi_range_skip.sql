$fetch = (
    SELECT * FROM Input1 WHERE (
        Group > 10
        OR
        Group = 10 AND Name > "Name2")
);

SELECT * FROM $fetch WHERE Amount != 105
ORDER BY Amount DESC, Comment LIMIT 3 OFFSET 2;
