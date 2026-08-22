PRAGMA DisableSimpleColumns;
USE plato;
SELECT i1.key AS Key, i1.value as Value, i2.value As Leaf, i3.value AS Branch, i4.value AS Branch_Leaf
FROM Roots AS i1
INNER JOIN Leaves AS i2 ON i1.leaf = i2.key
INNER JOIN Branches AS i3 ON i1.branch = i3.key
INNER JOIN Leaves AS i4 ON i3.leaf = i4.key;
