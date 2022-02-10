--!syntax_v1
PRAGMA AnsiInForEmptyOrNullableItemsCollections;

$keys = AsList("One", "Three");

SELECT COUNT(*)
FROM InputJoin1 AS j1
JOIN InputJoin2 AS j2
ON j1.Fk21 = j2.Key1
WHERE Key2 IN $keys;
