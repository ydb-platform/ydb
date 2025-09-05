PRAGMA DisableSimpleColumns;
USE plato;
select * from (
    SELECT DISTINCT i1.key AS Key, i1.value as Value, i2.value as Join
    FROM Roots AS i1
    INNER JOIN Leaves AS i2 ON i1.leaf = i2.key
    UNION ALL
    SELECT DISTINCT i1.key AS Key, i1.value as Value, i2.value as Join
    FROM Roots AS i1
    INNER JOIN Branches AS i2 ON i1.branch = i2.key
) order by Key, Value, Join;
