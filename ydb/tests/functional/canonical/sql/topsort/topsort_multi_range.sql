$fetch = (
    SELECT * FROM Input1 WHERE (
        Group > 10
        OR
        Group = 10 AND Name >= "Name3")
);

SELECT * FROM $fetch
ORDER BY Amount DESC, Group, Name LIMIT 5;
