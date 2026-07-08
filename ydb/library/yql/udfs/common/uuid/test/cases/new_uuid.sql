$p = Uuid::newPrefix();

SELECT Uuid::newV7() != Uuid::newV7() AS v7_unique;
SELECT Uuid::newV8() != Uuid::newV8() AS v8_unique;

SELECT Uuid::newV7($p) != Uuid::newV7($p) AS v7_prefix_unique;
SELECT Uuid::newV8($p) != Uuid::newV8() AS v8_prefix_differs;

SELECT $p != 0ul OR $p == 0ul AS prefix_is_uint64;
