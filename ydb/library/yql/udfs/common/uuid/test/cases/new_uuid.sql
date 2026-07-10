$p = RandomNumber(1);

SELECT Uuid::newV7() != Uuid::newV7() AS v7_unique;
SELECT Uuid::newV8() != Uuid::newV8() AS v8_unique;

SELECT Uuid::newPrefixV7($p) != Uuid::newPrefixV7($p) AS v7_prefix_unique;
SELECT Uuid::newPrefixV8($p) != Uuid::newV8() AS v8_prefix_differs;

SELECT Uuid::newPrefixV7(3) != Uuid::newV7() AS v7_small_prefix_differs;

SELECT Uuid::newV7(1) != Uuid::newV7(2) AS v7_dep_unique;
SELECT Uuid::newV8(1) != Uuid::newV8(2) AS v8_dep_unique;
SELECT Uuid::newV7(1, 2, 3) != Uuid::newV7(1, 2, 4) AS v7_three_dep_unique;
SELECT Uuid::newV8(1, 2, 3) != Uuid::newV8(1, 2, 4) AS v8_three_dep_unique;
SELECT Uuid::newPrefixV7($p, 1) != Uuid::newPrefixV7($p, 2) AS v7_prefix_dep_unique;
SELECT Uuid::newPrefixV8($p, 1) != Uuid::newPrefixV8($p, 2) AS v8_prefix_dep_unique;
SELECT Uuid::newPrefixV7($p, 1, 2, 3) != Uuid::newPrefixV7($p, 1, 2, 4) AS v7_prefix_three_dep_unique;
SELECT Uuid::newPrefixV8($p, 1, 2, 3) != Uuid::newPrefixV8($p, 1, 2, 4) AS v8_prefix_three_dep_unique;

SELECT
    Substring(CAST(Uuid::newV7() AS String), 8, 1) = '-'
    AND Substring(CAST(Uuid::newV7() AS String), 13, 1) = '-'
    AND Substring(CAST(Uuid::newV7() AS String), 18, 1) = '-'
    AND Substring(CAST(Uuid::newV7() AS String), 23, 1) = '-'
    AND Substring(CAST(Uuid::newV7() AS String), 14, 1) = '7'
    AS v7_string_format;
SELECT
    Substring(CAST(Uuid::newPrefixV7($p) AS String), 8, 1) = '-'
    AND Substring(CAST(Uuid::newPrefixV7($p) AS String), 13, 1) = '-'
    AND Substring(CAST(Uuid::newPrefixV7($p) AS String), 18, 1) = '-'
    AND Substring(CAST(Uuid::newPrefixV7($p) AS String), 23, 1) = '-'
    AND Substring(CAST(Uuid::newPrefixV7($p) AS String), 14, 1) = '7'
    AS v7_prefix_string_format;
SELECT
    Substring(CAST(Uuid::newV8() AS String), 8, 1) = '-'
    AND Substring(CAST(Uuid::newV8() AS String), 13, 1) = '-'
    AND Substring(CAST(Uuid::newV8() AS String), 18, 1) = '-'
    AND Substring(CAST(Uuid::newV8() AS String), 23, 1) = '-'
    AND Substring(CAST(Uuid::newV8() AS String), 14, 1) = '8'
    AS v8_string_format;
SELECT
    Substring(CAST(Uuid::newPrefixV8($p) AS String), 8, 1) = '-'
    AND Substring(CAST(Uuid::newPrefixV8($p) AS String), 13, 1) = '-'
    AND Substring(CAST(Uuid::newPrefixV8($p) AS String), 18, 1) = '-'
    AND Substring(CAST(Uuid::newPrefixV8($p) AS String), 23, 1) = '-'
    AND Substring(CAST(Uuid::newPrefixV8($p) AS String), 14, 1) = '8'
    AS v8_prefix_string_format;

SELECT $p != 0ul OR $p == 0ul AS prefix_is_uint64;
