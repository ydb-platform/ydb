$p = RandomNumber(1);

SELECT Uuid::newV7() != Uuid::newV7() AS v7_unique;
SELECT Uuid::newV8() != Uuid::newV8() AS v8_unique;

SELECT Uuid::newPrefixV7($p) != Uuid::newPrefixV7($p) AS v7_prefix_unique;
SELECT Uuid::newPrefixV8($p) != Uuid::newV8() AS v8_prefix_differs;

SELECT Uuid::newV7(1) != Uuid::newV7(2) AS v7_dep_unique;
SELECT Uuid::newPrefixV7($p, 1) != Uuid::newPrefixV7($p, 2) AS v7_prefix_dep_unique;

SELECT $p != 0ul OR $p == 0ul AS prefix_is_uint64;
