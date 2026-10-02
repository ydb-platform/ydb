PRAGMA DisableSimpleColumns;
USE plato;
SELECT i1.key AS Key, i1.value AS Value, i2.value AS Leaf, i3.value AS Branch
FROM Roots AS i1
INNER JOIN Leaves AS i2 ON i1.leaf = i2.key
INNER JOIN Branches AS i3 ON i1.branch = i3.key
ORDER BY Leaf, Branch;
